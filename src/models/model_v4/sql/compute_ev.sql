-- ============================================================================
-- EV / Edge Calculator
-- ----------------------------------------------------------------------------
-- Run this AFTER you've manually filled in `sportsbook`, `line`, `over_odds`,
-- and `under_odds` for the rows you care about in
--   dbo.fact_pitcher_strikeout_betting_ev
--
-- It will compute (in place):
--   * model_prob_over / model_prob_under   - from the predictions table
--   * implied_prob_over / implied_prob_under - 1 / decimal_odds
--   * edge_over / edge_under                - model_prob - implied_prob
--   * ev_over / ev_under                    - expected return on a $1 bet
--   * recommended_side                      - 'OVER' / 'UNDER' / 'PASS'
--   * kelly_fraction                        - quarter-Kelly stake suggestion
--
-- Australian decimal odds (e.g. 1.90, 2.05) are assumed.
-- ============================================================================

-- ============================================================================
-- BIAS CORRECTION
-- ----------------------------------------------------------------------------
-- The model may over-predict strikeouts due to:
--   (a) Slightly inflated per-PA K probabilities from training
--   (b) 27 synthetic PAs per game (3 x 9) vs ~18-24 real starter PAs
--
-- Run the DIAGNOSTIC QUERY at the bottom of this script to measure the
-- historical bias. Then set @BIAS_KS to the nearest 0.5:
--
--   Measured bias | Set @BIAS_KS
--   ------------- | ------------
--   0.0 – 0.3     |  0.0  (no correction)
--   0.3 – 0.7     |  0.5
--   0.7 – 1.2     |  1.0
--   1.2 – 1.7     |  1.5
--   > 1.7         |  2.0
--
-- A positive @BIAS_KS shifts the effective line UP, which lowers model_prob_over
-- (makes OVER recommendations more conservative).
-- ============================================================================
DECLARE @BIAS_KS FLOAT = 0.0;  -- tune from the diagnostic query if bias persists after retraining

-- Step 1: pull model probabilities for the chosen line, from the game predictions
-- table. We pick the matching prob_over_X column based on `line + @BIAS_KS`.
-- This corrects for model upward bias: if the model over-predicts by 1 K,
-- look up the probability at (line + 1) instead of at line.
UPDATE ev
SET
    model_prob_over =
        CASE CAST(ev.line + @BIAS_KS AS DECIMAL(4,1))
            WHEN 3.5 THEN g.prob_over_3_5
            WHEN 4.5 THEN g.prob_over_4_5
            WHEN 5.5 THEN g.prob_over_5_5
            WHEN 6.5 THEN g.prob_over_6_5
            WHEN 7.5 THEN g.prob_over_7_5
            WHEN 8.5 THEN g.prob_over_8_5
            WHEN 9.5 THEN g.prob_over_9_5
            ELSE NULL
        END
FROM dbo.fact_pitcher_strikeout_betting_ev ev
JOIN dbo.fact_pitcher_game_strikeout_predictions g
  ON g.gamePk = ev.gamePk
 AND g.pitcher_id = ev.pitcher_id
WHERE ev.over_odds IS NOT NULL;

UPDATE dbo.fact_pitcher_strikeout_betting_ev
SET model_prob_under = 1.0 - model_prob_over
WHERE model_prob_over IS NOT NULL;

-- Step 2: implied probabilities from sportsbook odds.
-- Decimal odds: implied = 1 / odds.
UPDATE dbo.fact_pitcher_strikeout_betting_ev
SET
    implied_prob_over  = 1.0 / NULLIF(over_odds, 0),
    implied_prob_under = 1.0 / NULLIF(under_odds, 0)
WHERE over_odds IS NOT NULL OR under_odds IS NOT NULL;

-- Step 3: edge = our probability - book's implied probability.
UPDATE dbo.fact_pitcher_strikeout_betting_ev
SET
    edge_over  = model_prob_over  - implied_prob_over,
    edge_under = model_prob_under - implied_prob_under
WHERE model_prob_over IS NOT NULL;

-- Step 4: expected value of a $1 bet.
-- EV = (model_prob * (odds - 1)) - (1 - model_prob)
UPDATE dbo.fact_pitcher_strikeout_betting_ev
SET
    ev_over  = (model_prob_over  * (over_odds  - 1.0)) - (1.0 - model_prob_over),
    ev_under = (model_prob_under * (under_odds - 1.0)) - (1.0 - model_prob_under)
WHERE model_prob_over IS NOT NULL;

-- Step 5: recommend a side. Only flag a bet if EV > 0.05 (5%) - real edges
-- under that get eaten by variance + line movement. Otherwise PASS.
UPDATE dbo.fact_pitcher_strikeout_betting_ev
SET recommended_side =
    CASE
        WHEN ev_over  > 0.05 AND ev_over  > ev_under THEN 'OVER'
        WHEN ev_under > 0.05 AND ev_under > ev_over  THEN 'UNDER'
        ELSE 'PASS'
    END
WHERE ev_over IS NOT NULL OR ev_under IS NOT NULL;

-- Step 6: quarter-Kelly bet sizing.
-- Full Kelly = (b*p - q) / b where b = odds-1, p = model prob, q = 1-p.
-- We use a quarter of that. Kelly is too aggressive in practice given
-- model uncertainty, and quarter-Kelly is a common compromise.
UPDATE dbo.fact_pitcher_strikeout_betting_ev
SET kelly_fraction =
    CASE
        WHEN recommended_side = 'OVER' THEN
            0.25 * (((over_odds - 1.0) * model_prob_over - (1.0 - model_prob_over))
                    / NULLIF(over_odds - 1.0, 0))
        WHEN recommended_side = 'UNDER' THEN
            0.25 * (((under_odds - 1.0) * model_prob_under - (1.0 - model_prob_under))
                    / NULLIF(under_odds - 1.0, 0))
        ELSE 0
    END
WHERE recommended_side IS NOT NULL;

-- Floor Kelly at 0 (never bet a negative size).
UPDATE dbo.fact_pitcher_strikeout_betting_ev
SET kelly_fraction = 0
WHERE kelly_fraction < 0;

-- Step 7: settle bet results once games are played.
UPDATE dbo.fact_pitcher_strikeout_betting_ev
SET bet_result =
    CASE
        WHEN actual_strikeouts IS NULL THEN NULL
        WHEN recommended_side = 'OVER'  AND actual_strikeouts > line THEN 'WIN'
        WHEN recommended_side = 'OVER'  AND actual_strikeouts < line THEN 'LOSS'
        WHEN recommended_side = 'UNDER' AND actual_strikeouts < line THEN 'WIN'
        WHEN recommended_side = 'UNDER' AND actual_strikeouts > line THEN 'LOSS'
        WHEN actual_strikeouts = line THEN 'PUSH'
        ELSE NULL
    END
WHERE actual_strikeouts IS NOT NULL AND recommended_side IS NOT NULL;

-- ============================================================================
-- Quick reporting query
-- ============================================================================
SELECT
    pitcher_name,
    opponent_team_name,
    game_date,
    line,
    predicted_strikeouts,
    model_prob_over,
    over_odds,
    edge_over,
    ev_over,
    recommended_side,
    kelly_fraction,
    actual_strikeouts,
    bet_result
FROM dbo.fact_pitcher_strikeout_betting_ev
WHERE recommended_side IN ('OVER','UNDER')
ORDER BY game_date, ev_over DESC;

-- ============================================================================
-- DIAGNOSTIC: Measure model bias (run this to calibrate @BIAS_KS above)
-- ----------------------------------------------------------------------------
-- Compare predicted vs actual strikeouts for all historical games.
-- avg_bias > 0 means the model over-predicts; use that value to set @BIAS_KS.
-- ============================================================================
SELECT
    g.split_set,
    COUNT(*)                                                       AS games,
    ROUND(AVG(g.predicted_strikeouts), 2)                         AS avg_predicted,
    ROUND(AVG(CAST(g.actual_strikeouts AS FLOAT)), 2)             AS avg_actual,
    ROUND(AVG(g.predicted_strikeouts - g.actual_strikeouts), 2)   AS avg_bias,
    ROUND(SQRT(AVG(POWER(g.predicted_strikeouts - g.actual_strikeouts, 2))), 2) AS rmse,
    -- Break down bias by predicted bucket to see where it concentrates
    ROUND(AVG(CASE WHEN g.predicted_strikeouts < 5   THEN g.predicted_strikeouts - g.actual_strikeouts END), 2) AS bias_under_5k,
    ROUND(AVG(CASE WHEN g.predicted_strikeouts BETWEEN 5 AND 7 THEN g.predicted_strikeouts - g.actual_strikeouts END), 2) AS bias_5_to_7k,
    ROUND(AVG(CASE WHEN g.predicted_strikeouts > 7   THEN g.predicted_strikeouts - g.actual_strikeouts END), 2) AS bias_over_7k
FROM dbo.fact_pitcher_game_strikeout_predictions g
WHERE g.actual_strikeouts IS NOT NULL
GROUP BY g.split_set
ORDER BY g.split_set;

-- Also check EV bet result summary (how OVER vs UNDER is actually performing)
SELECT
    recommended_side,
    COUNT(*)                                      AS total_bets,
    SUM(CASE WHEN bet_result = 'WIN'  THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN bet_result = 'LOSS' THEN 1 ELSE 0 END) AS losses,
    SUM(CASE WHEN bet_result = 'PUSH' THEN 1 ELSE 0 END) AS pushes,
    ROUND(100.0 * SUM(CASE WHEN bet_result = 'WIN' THEN 1 ELSE 0 END)
               / NULLIF(SUM(CASE WHEN bet_result IN ('WIN','LOSS') THEN 1 ELSE 0 END), 0), 1) AS win_pct
FROM dbo.fact_pitcher_strikeout_betting_ev
WHERE bet_result IS NOT NULL
  AND recommended_side IN ('OVER','UNDER')
GROUP BY recommended_side;