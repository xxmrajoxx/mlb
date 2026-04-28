Smart question to ask. The honest answer: 2 rows isn't necessarily disqualifying — but for a different reason than you might think.
What "rows" means here matters
Your two rows for Anthony Kay are two pitcher-games in the test set (2026 season). That's not the same as "the model has only seen 2 data points about him."
The model learned about pitching during training. It's now applying what it learned to predict Anthony Kay's specific outings. The 2 rows you see are just how many 2026 starts he's made so far.
What matters more is how much historical data the model had about Anthony Kay during training (2023-2024).
The two questions you actually need to answer
Question 1: Did the model train on enough Anthony Kay data?
If Kay made 25 starts in 2023+2024 with full stat tracking, the model has rich knowledge of him. If he was a Triple-A guy who just got promoted with no big-league history, the model is essentially guessing from his peripheral stats.
Run this query to find out:
sqlSELECT
    season,
    COUNT(DISTINCT gamePk) AS games,
    COUNT(*) AS hitter_pa_rows,
    SUM(hitter_plate_appearances) AS total_pas
FROM mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2
WHERE pitcher_name = 'Anthony Kay'
GROUP BY season
ORDER BY season;
What you want to see:

15+ games in 2023 or 2024 → model has solid knowledge of him → bettable
5-15 games → model has decent knowledge → bet smaller
0-5 games → model is guessing from his peripheral stats → skip

Question 2: Is the per-game prediction itself reliable?
Even if the model knows the pitcher well, individual predictions vary in confidence. Look at predicted_k_stddev — the model's own uncertainty estimate.
For Kay's two games:

April 9 vs Royals: stddev = 1.94 (acceptable)
March 29 vs Brewers: stddev = 1.69 (good)

Both are in the moderate-confidence range. The model isn't flagging unusual uncertainty here.
Reading Kay's actual track record
Looking at the two games you have:
GamePredictedActualMissApril 9 vs Royals4.696-1.31March 29 vs Brewers3.515-1.49
The model is under-predicting Kay by about 1.4 Ks per game. Two games is way too small a sample to call this a real pattern (could just be variance), but it's worth noting.
So when CAN you bet on him?
Three things would make me comfortable betting on Anthony Kay's next start:
1. Confirm sufficient training history. Run the query above. If he has 15+ starts in 2023-2024, the model has real knowledge of him.
2. The bet passes the EV script. When you enter odds and run compute_ev.sql, if it returns OVER or UNDER, the script has already verified edge ≥ 6% and EV ≥ 7%. That's a meaningful bar regardless of pitcher familiarity.
3. The prediction is in a reliable bucket (predicted < 8 K) and has reasonable stddev (< 2.2). Both of Kay's games pass this.
The framework I'd actually recommend
Forget "rows in the prediction table." That's not what determines bet quality. Use this hierarchy instead:
CheckWhat it tells youWhat to do if it failsDid the EV script recommend OVER or UNDER?The model thinks there's an edge worth bettingSkip — script knows what it's doingIs predicted_strikeouts < 8.0?We're not in the unreliable high-K bucketSkip — auto-PASSed anywayIs predicted_k_stddev < 2.2?Model isn't flagging unusual uncertaintyReduce stake or skipHas the pitcher made 15+ MLB starts in 2023-2025?Model has training knowledge of himReduce stake to 1-2% (instead of full Kelly)Is the lineup confirmed?Late scratches don't invalidate the predictionWait until lineups postIs bankroll exposure for today < 15%?You're not over-betting one slateSkip remaining bets
If all six pass, place the bet. If the pitcher-history check fails (rookie, just promoted, recently traded), still bet but cut the stake in half.
A useful sanity-check query for any pitcher before betting
Bookmark this — run it for any pitcher whose name you don't recognize:
sqlSELECT
    p.pitcher_name,
    SUM(CASE WHEN p.season IN (2023, 2024) THEN 1 ELSE 0 END) AS train_pa_rows,
    COUNT(DISTINCT CASE WHEN p.season IN (2023, 2024) THEN p.gamePk END) AS train_games,
    SUM(CASE WHEN p.season = 2025 THEN 1 ELSE 0 END) AS validation_pa_rows,
    COUNT(DISTINCT CASE WHEN p.season = 2025 THEN p.gamePk END) AS validation_games,
    COUNT(DISTINCT CASE WHEN p.season = 2026 THEN p.gamePk END) AS test_games_so_far
FROM mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2 p
WHERE p.pitcher_name = 'Anthony Kay'
GROUP BY p.pitcher_name;
Decision rules from the result:
train_games (2023-24)What to do20+Bet normally if EV script approves10-20Bet at slightly reduced stake (~75% of suggested Kelly)5-10Bet at half stake, only on edges > 10%0-5Skip. Model has too little history to be reliable.
TL;DR
The "2 rows" thing isn't really the issue. The issue is whether the model trained on enough Anthony Kay data to make reliable predictions about him.
Run the sanity-check query above. If he has 15+ games in your training seasons, the predictions are trustworthy and you can let the EV script decide. If he's a rookie or fringe player with under 5 training games, skip regardless of what the script says.
This rule applies to every pitcher — not just Kay. It's worth getting in the habit of running that query for any pitcher whose name doesn't ring a bell before placing a bet.


---------------------------- # predicted_k_stdeev ----------------------------
for question 2 predicted_k_stdeev - what is a good range
7:40 pm
Claude responded: Good follow-up.

Good follow-up. Let me explain what predicted_k_stddev actually means and give you proper ranges.
What predicted_k_stddev actually measures

It's the model's own uncertainty estimate for the predicted K total. Specifically, it's the standard deviation of the Poisson-binomial distribution we computed when summing per-batter probabilities.

In plain terms: how spread out the possible outcomes are around the predicted mean.

A pitcher with predicted_strikeouts = 6.0 and predicted_k_stddev = 1.5 means:

    Most likely outcome is around 6 Ks
    The realistic range of outcomes is roughly 4-8 Ks (predicted ± 1 stddev covers ~68%, ± 2 stddev covers ~95%)

Why it varies

Stddev isn't fixed across pitchers — it depends on the per-PA probabilities and how many batters faced. Mathematically:

stddev = sqrt(sum of p × (1-p) for each PA)

This means stddev is highest when:

    The pitcher faces many batters (more PAs = more variance accumulated)
    Per-PA K probabilities are around 0.5 (max uncertainty per coin flip)

Stddev is lowest when:

    The pitcher faces few batters (short outing prediction)
    Per-PA probabilities are extreme (close to 0 or 1)

The actual ranges

For typical MLB starting pitcher predictions, here's what stddev values mean:
Stddev	Verdict	What to do
< 1.4	Very tight prediction	Bet normally — model is confident
1.4 – 1.8	Healthy range — typical for most starts	Bet normally
1.8 – 2.1	Acceptable — slightly more variance than usual	Bet normally
2.1 – 2.4	Wider variance — bet with some caution	Reduce stake to 75% of suggested Kelly
2.4 – 2.7	High variance — meaningful uncertainty	Halve the stake
> 2.7	Very high variance — the model is genuinely uncertain	Skip the bet
Why this matters for betting

The further the line is from the predicted mean, the more stddev affects your edge.

Example 1 — line far from prediction (stddev matters less):

    Predicted: 7.5 K, Stddev: 2.0
    Line: 5.5
    Going OVER means K must exceed 5.5 — that's 1.0 stddev below the mean
    Even with high variance, going over 5.5 is very likely (~84% probability)
    Stddev of 2.0 vs 1.5 doesn't change much

Example 2 — line near prediction (stddev matters a lot):

    Predicted: 5.5 K, Stddev: 2.0
    Line: 5.5
    Going OVER means K must exceed the predicted mean — coin flip
    The probability swings significantly with stddev: 1.5 stddev gives ~50%, 2.5 stddev gives ~50% but with much wider tails
    The bet is essentially on noise; pass

The rule: the closer the line is to the prediction, the more stddev hurts your bet quality.
The combined-signal check

Stddev alone isn't enough — combine it with how far the line sits from the prediction. Calculate this in your head:

distance_from_line = | predicted_strikeouts - line | / predicted_k_stddev

This is a "z-score" telling you how many standard deviations the line is from the prediction.
Distance	Interpretation
< 0.5	Line is right on the prediction — pure noise. Skip.
0.5 – 1.0	Line is mildly off — borderline edge
1.0 – 1.5	Line is well off — comfortable bet
1.5 – 2.0	Line is significantly off — strong bet
> 2.0	Line is way off — either a great edge or a stale line

Worked example: Predicted 6.5, stddev 1.8, line 5.5

distance = |6.5 - 5.5| / 1.8 = 0.56

That's a borderline bet. The line is barely outside one stddev of the prediction. Even if the EV script approves it, expect more variance than the EV math suggests.

Better example: Predicted 7.5, stddev 1.6, line 5.5

distance = |7.5 - 5.5| / 1.6 = 1.25

Now the line is well off the prediction. This is a confident OVER bet.
A sanity check before each bet

Add this to your pre-bet routine:
sql

SELECT
    pitcher_name,
    line,
    predicted_strikeouts,
    predicted_k_stddev,
    ABS(predicted_strikeouts - line) / predicted_k_stddev AS distance_from_line,
    recommended_side,
    ev_over,
    ev_under
FROM mlb.dbo.fact_pitcher_strikeout_betting_ev
WHERE recommended_side IN ('OVER', 'UNDER')
  AND game_date = CAST(GETDATE() AS DATE);

If distance_from_line < 0.7, treat it as a borderline bet regardless of what the EV script says. The script accounts for stddev when computing probabilities, but a thin distance amplifies any model error.
Anthony Kay's specific case

Looking at his two games:

    April 9: stddev = 1.94 (Acceptable, slightly higher than typical)
    March 29: stddev = 1.69 (Healthy range)

Both stddevs are fine. The model isn't flagging unusual uncertainty about Kay's K totals.

Combined with the under-predictions you saw (model said 4.7 and 3.5, actual was 6 and 5), the picture is: the model is reasonably confident about Kay but might be slightly low on him. That's a one-bet-at-a-time observation, not a confident pattern from 2 games.
TL;DR for stddev

    1.4–2.1 is the healthy range — most starter predictions live here
    Below 1.4 = very confident prediction, often short outings
    Above 2.4 = the model is uncertain, reduce stake or skip
    Always check distance-from-line alongside stddev — both matter together
