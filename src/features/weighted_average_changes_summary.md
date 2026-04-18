# Weighted Average Changes Summary

## Purpose
Before building the models, the rolling feature tables were updated so that the **weighted averages use volume** instead of the old **manual recency weights**.

Previously, several scripts used weighting patterns like:
- `0.50 / 0.30 / 0.20`
- `5,4,3,2,1`
- `10,9,8,...,1`
- row-number weighting in self-join logic (`wavg_*` columns)

These were removed and replaced with **volume-based weighting**, so features now better reflect both:
- recent sample size
- true opportunity / exposure

---

## Main Design Change

### Old approach
Weighted averages were based on **position in the rolling window**.

Example:
```sql
0.50 * last_game + 0.30 * two_games_ago + 0.20 * three_games_ago
```

### New approach
Weighted averages are now based on **volume**.

General pattern:
```sql
SUM(metric * volume) / SUM(volume)
```

This means a game with more relevant opportunity contributes more than a tiny sample game.

---

# 1) `mlb.dbo.fact_pitcher_statcast_rolling_features`

## What changed
- Removed the old manual weighted sections:
  - weighted last 3 with `0.50/0.30/0.20`
  - weighted last 5 with `5,4,3,2,1`
  - weighted last 10 with `10 -> 1`
- Kept the **simple rolling averages**
- Replaced weighted averages with **volume-weighted statcast rates**

## Volume used
- **`total_pitches`**

## Why
This is a pitch-level statcast table, so pitch count is the correct opportunity denominator.

## New weighted feature style
Examples:
- `weighted_whiff_rate_last_3/5/10`
- `weighted_called_strike_rate_last_3/5/10`
- `weighted_csw_rate_last_3/5/10`
- `weighted_sc_strike_rate_last_3/5/10`
- `weighted_fps_rate_last_3/5/10`
- `weighted_putaway_rate_last_3/5/10`
- `weighted_swing_rate_last_3/5/10`
- `weighted_chase_rate_last_3/5/10`
- `weighted_zone_rate_last_3/5/10`
- pitch-type usage and pitch-type whiff weighted columns
- handedness split weighted columns

## Intentionally not volume-weighted
These were left as simple averages:
- velocity
- spin rate
- extension
- movement
- plate location summaries
- EV allowed type physical descriptors

Reason: those are better treated as rolling averages than pitch-volume weighted rates.

---

# 2) `mlb.dbo.fact_pitcher_rolling_features`

## What changed
- Removed the old manual weighted columns based on recency-only weighting
- Kept the **simple rolling averages**
- Replaced weighted logic with **volume-weighted pitching efficiency / opportunity rates**

## Volume used
Different denominators were used depending on the metric:

### `battersFaced`
Used for pitcher opportunity / outcome rates:
- strikeout rate
- walk rate
- hit rate style features
- HR allowed per batter faced

### `numberOfPitches`
Used for strike / pitch-efficiency style features:
- strike percentage style features

### `outs`
Used for inning-normalized features:
- K/9
- BB/9
- pitches per inning
- WHIP-style rate reconstruction

### `atBats`
Used for batting average allowed style features

## New weighted feature style
Examples:
- `weighted_k_per_bf_last_3/5/10`
- `weighted_bb_per_bf_last_3/5/10`
- `weighted_baa_last_3/5/10`
- `weighted_hr_per_bf_last_3/5/10`
- `weighted_strike_pct_last_3/5/10`
- `weighted_pitches_per_inning_last_3/5/10`
- `weighted_k9_last_3/5/10`
- `weighted_bb9_last_3/5/10`
- `weighted_whip_last_3/5/10`
- `weighted_kbb_last_3/5/10`
- `weighted_inherited_runner_score_pct_last_3/5/10`

## Important modeling benefit
This table now gives:
- raw rolling averages for form
- volume-weighted rate features for reliability

That is much better for XGBoost than the old recency-only weighted numbers.

---

# 3) `mlb.dbo.fact_hitter_statcast_rolling_features`

## What changed
- Removed the old row-number weighted `wavg_*` logic
- Kept the **simple rolling averages**
- Replaced weighted averages with **volume-weighted hitter statcast rates**

## Volume used
- **`total_pitches_seen`**

## Why
This is the hitter statcast table, so pitch exposure is the correct denominator for swing / whiff / chase / seen-pitch mix features.

## New weighted feature style
Examples:
- `weighted_whiff_rate_last_3/5/10`
- `weighted_contact_rate_last_3/5/10`
- `weighted_swing_rate_last_3/5/10`
- `weighted_chase_rate_last_3/5/10`
- `weighted_zone_swing_rate_last_3/5/10`
- `weighted_zone_rate_last_3/5/10`
- `weighted_called_strike_rate_last_3/5/10`
- `weighted_csw_against_rate_last_3/5/10`
- `weighted_two_strike_whiff_rate_last_3/5/10`
- count-specific whiff weighted columns
- seen pitch-type distribution weighted columns
- handedness split weighted columns

## Intentionally left as simple averages
These were not converted to volume-weighted features:
- exit velocity
n- max exit velocity
- launch angle
- hit distance
- xBA
- xwOBA
- wOBA value
- BABIP value
- ISO value
- bat speed
- swing length
- pitch characteristic seen summaries

Reason: these are descriptive quality/shape metrics and are better left as standard rolling averages.

---

# 4) `mlb.dbo.fact_hitter_rolling_features`

## What changed
- Removed the old recency-style `wavg_*` logic
- Kept the **simple rolling averages**
- Replaced weighted logic with **volume-weighted hitter outcome rates**

## Volume used
Different denominators were used depending on the metric:

### `plateAppearances`
Used for hitter opportunity/outcome rates:
- K rate
- walk rate
- hit rate
- total base rate
- HR rate
- OBP
- OPS
- pitches per PA

### `atBats`
Used for AB-based quality stats:
- batting average
- SLG
- BABIP

## New weighted feature style
Examples:
- `weighted_k_rate_last_3/5/10`
- `weighted_walk_rate_last_3/5/10`
- `weighted_hit_rate_last_3/5/10`
- `weighted_tb_rate_last_3/5/10`
- `weighted_hr_rate_last_3/5/10`
- `weighted_batting_avg_last_3/5/10`
- `weighted_pitches_per_pa_last_3/5/10`
- `weighted_obp_last_3/5/10`
- `weighted_slg_last_3/5/10`
- `weighted_ops_last_3/5/10`
- `weighted_babip_last_3/5/10`

## Important modeling benefit
This table now better reflects whether a hitter’s recent form came from:
- real opportunity volume
- or just a tiny sample of plate appearances

---

# Overall Impact on Modeling

## Why this is better
The old approach treated:
- a tiny appearance
- and a full workload appearance

as if they carried similar importance, just because they were recent.

The new approach makes the rolling features much more reliable.

## Modeling improvement
These changes should improve feature quality for:
- hitter strikeout models
- pitcher strikeout models
- hitter vs pitcher matchup models
- future betting recommendation pipelines

Because the model now sees:
- simple rolling averages = recent form
- volume-weighted rolling rates = reliable recent form

---

# Important Follow-Up
After these changes, any downstream tables that reference the old weighted column names may need to be updated.

Most likely tables to review next:
- `mlb.dbo.fact_pitcher_model_features`
- `mlb.dbo.fact_hitter_model_features`
- `mlb.dbo.fact_hitter_pitcher_matchup_model_features`

Reason:
- some old `weighted_*` columns were renamed
- some old recency-weighted columns were removed entirely
- new weighted columns are now more rate-based and denominator-aware

---

# Recommended Next Step
Before starting the model build, update the final model feature tables so they pull in the new weighted columns from:
- `fact_pitcher_statcast_rolling_features`
- `fact_pitcher_rolling_features`
- `fact_hitter_statcast_rolling_features`
- `fact_hitter_rolling_features`

That will keep the full feature stack aligned.
