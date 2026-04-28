-- ============================================================================
-- Create a view that enriches the source feature table with handedness data
-- ----------------------------------------------------------------------------
-- This view ADDS three columns to the existing matchup-features table:
--   * pitcher_throws       'L' or 'R'
--   * hitter_bats          'L', 'R', or 'S' (switch)
--   * is_same_side_matchup 1 if same-side, 0 if opposite, NULL if switch
--
-- It also adds simple platoon flags useful for tree splits.
--
-- After running this, point the pipeline at this view instead of the raw
-- features table. Just one line change in config.py.
-- ============================================================================

USE mlb;
GO

-- DROP / CREATE VIEW must be unqualified - SQL Server doesn't allow the
-- database prefix on these statements (the database context comes from USE).
IF OBJECT_ID('dbo.fact_hitter_pitcher_matchup_with_handedness', 'V') IS NOT NULL
    DROP VIEW dbo.fact_hitter_pitcher_matchup_with_handedness;
GO

CREATE VIEW dbo.fact_hitter_pitcher_matchup_with_handedness AS
SELECT
    f.*,
    -- Direct handedness codes (the most important new features)
    h.bat_side_code      AS hitter_bats,        -- 'L', 'R', or 'S'
    p.pitch_hand_code    AS pitcher_throws,     -- 'L' or 'R'

    -- Categorical platoon flag (nice for tree splits)
    CASE
        WHEN h.bat_side_code = 'S' THEN 'Switch'
        WHEN h.bat_side_code = p.pitch_hand_code THEN 'Same'
        WHEN h.bat_side_code IS NOT NULL AND p.pitch_hand_code IS NOT NULL THEN 'Opposite'
        ELSE NULL
    END AS platoon_matchup,

    -- Binary same-side flag - 1 / 0 / NULL
    -- NULL for switch hitters since they effectively get to choose
    CASE
        WHEN h.bat_side_code = 'S' THEN NULL
        WHEN h.bat_side_code = p.pitch_hand_code THEN 1
        WHEN h.bat_side_code IS NOT NULL AND p.pitch_hand_code IS NOT NULL THEN 0
        ELSE NULL
    END AS is_same_side_matchup,

    -- Two boolean flags - more explicit signal for the tree models
    CASE WHEN h.bat_side_code = 'L' THEN 1 ELSE 0 END AS hitter_is_lefty,
    CASE WHEN p.pitch_hand_code = 'L' THEN 1 ELSE 0 END AS pitcher_is_lefty,
    CASE WHEN h.bat_side_code = 'S' THEN 1 ELSE 0 END AS hitter_is_switch

FROM dbo.fact_hitter_pitcher_matchup_model_featuresv2 f
LEFT JOIN dbo.dim_player h ON h.player_id = f.hitter_id
LEFT JOIN dbo.dim_player p ON p.player_id = f.pitcher_id;
GO

-- ============================================================================
-- Quick verification - safe to qualify with mlb.dbo. on SELECT statements
-- ============================================================================
SELECT
    COUNT(*)                                                  AS total_rows,
    SUM(CASE WHEN hitter_bats     IS NULL THEN 1 ELSE 0 END)  AS missing_hitter_bats,
    SUM(CASE WHEN pitcher_throws  IS NULL THEN 1 ELSE 0 END)  AS missing_pitcher_throws,
    SUM(CASE WHEN hitter_bats = 'L' THEN 1 ELSE 0 END)        AS lhb_rows,
    SUM(CASE WHEN hitter_bats = 'R' THEN 1 ELSE 0 END)        AS rhb_rows,
    SUM(CASE WHEN hitter_bats = 'S' THEN 1 ELSE 0 END)        AS switch_rows,
    SUM(CASE WHEN pitcher_throws = 'L' THEN 1 ELSE 0 END)     AS lhp_rows,
    SUM(CASE WHEN pitcher_throws = 'R' THEN 1 ELSE 0 END)     AS rhp_rows
FROM mlb.dbo.fact_hitter_pitcher_matchup_with_handedness;

-- Sanity check: same-side matchups should have higher K rates than opposite
SELECT
    platoon_matchup,
    COUNT(*) AS n,
    AVG(CAST(hitter_strikeouts AS FLOAT) / NULLIF(hitter_plate_appearances, 0)) AS avg_k_rate
FROM mlb.dbo.fact_hitter_pitcher_matchup_with_handedness
WHERE hitter_plate_appearances > 0
  AND season >= 2024
GROUP BY platoon_matchup
ORDER BY platoon_matchup;
-- Expected: Same-side K rate ~2-4 percentage points higher than Opposite
