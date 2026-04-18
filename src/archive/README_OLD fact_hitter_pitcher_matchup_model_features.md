# MLB Feature Table Documentation

## `mlb.dbo.fact_hitter_pitcher_matchup_model_features`

---

## 🎯 Purpose

This table is the **primary feature dataset** used for Machine Learning Model 1:

> **Pitcher vs Hitter Strikeout Prediction**

Each row represents a **single matchup between a hitter and a pitcher in a specific game**, with all relevant features and the correct outcome.

---

## 🔄 Key Change (Before vs After)

### ❌ Previous Design (Incorrect Grain)

* **1 row per hitter per game**
* Only the **primary pitcher** was kept
* Used:

  ```sql
  ROW_NUMBER() OVER (PARTITION BY gamePk, hitter_id)
  WHERE rn = 1
  ```
* Target:

  ```sql
  hitter_strikeOuts = total strikeouts in game
  ```

### ⚠️ Problems

* Lost bullpen matchups
* Incorrect attribution of strikeouts
* Feature mismatch (pitcher vs hitter)

---

### ✅ New Design (Correct Grain)

* **1 row per hitter + pitcher + game**
* Keeps **ALL pitchers faced**
* Removes `primary_matchup` logic
* Target is now **pitcher-specific**

---

## 📊 Table Grain

> **1 row = (gamePk, hitter_id, pitcher_id)**

Example:

| gamePk | hitter     | pitcher  |
| ------ | ---------- | -------- |
| 823810 | Benintendi | Sproat   |
| 823810 | Benintendi | Anderson |
| 823810 | Benintendi | Woodford |

---

## 🎯 Target Variable

```sql
pa.strikeouts AS hitter_strikeOuts
```

### Meaning:

> Number of times the hitter struck out **against that specific pitcher in that game**

---

## 🧱 Data Sources

### 1. Hitter Features

From:

* `mlb.dbo.fact_hitter_model_features`

Includes:

* Rolling averages (last 3 / 5 / 10)
* Weighted averages
* Statcast metrics
* Previous game stats

---

### 2. Pitcher Features

From:

* `mlb.dbo.fact_pitcher_model_features`

Includes:

* Strikeout trends
* Pitch counts
* Whiff / CSW rates
* Velocity & movement
* Efficiency metrics

---

### 3. Matchup Features (Pitch-Level)

From:

* Statcast aggregation

Includes:

* `pitches_seen_vs_pitcher`
* `swings_vs_pitcher`
* `whiffs_vs_pitcher`
* `called_strikes_vs_pitcher`
* `matchup_whiff_rate`
* `matchup_called_strike_rate`
* `matchup_csw_rate`

---

### 4. Pitcher-Specific Outcomes

From:

* `mlb.dbo.fact_hitter_pitcher_pa_game_agg`

Includes:

* `plate_appearances`
* `strikeouts`
* `hits`
* `walks`
* `home_runs`
* `outs_recorded`

---

### 5. Lineup Context (NEW 🚀)

From:

* `mlb.dbo.fact_lineup_agg_features`

Includes:

* `lineup_avg_k_last_5`
* `lineup_avg_whiff_rate_last_5`
* `lineup_avg_ops_last_5`
* `lineup_wavg_k_last_5`
* `lineup_wavg_whiff_rate_last_5`
* `lineup_num_high_k_hitters`
* `lineup_num_power_hitters`
* `lineup_num_high_whiff_hitters`

---

### 6. Lineup Position

From:

* `mlb.dbo.fact_hitter_lineup`

Includes:

* batting order
* lineup position

---

## 🧠 Feature Layers

The table combines 4 key layers:

| Layer   | Description                            |
| ------- | -------------------------------------- |
| Hitter  | Player form & hitting ability          |
| Pitcher | Pitching form & strikeout ability      |
| Matchup | Direct interaction (pitch-level stats) |
| Team    | Lineup strength & strikeout tendencies |

---

## 🚀 Why This Design is Better

### ✔ Correct Target Alignment

* Strikeouts are now tied to the **actual pitcher faced**

### ✔ No Data Loss

* All pitchers (including bullpen) are included

### ✔ Stronger Features

* Combines micro (matchup) + macro (team context)

### ✔ Ideal for ML

* Clean supervised learning structure:

  ```
  X = features (hitter + pitcher + matchup + lineup)
  y = hitter_strikeOuts
  ```

---

## 🧪 Model Usage

### Model 1: Pitcher vs Hitter

**Goal:**

> Predict probability that a hitter strikes out vs a pitcher

### Example Output

| Batter   | Pitcher   | K Probability |
| -------- | --------- | ------------- |
| Batter 1 | Pitcher A | 0.30          |
| Batter 2 | Pitcher A | 0.25          |
| Batter 3 | Pitcher A | 0.40          |

---

### Team-Level Projection

```text
Expected Ks = SUM(hitter strikeout probabilities)
```

---

## 🔥 Summary

> The table evolved from a **hitter-game table with a single pitcher**
> into a **true hitter-pitcher-game feature dataset**
> with correct targets and full matchup coverage.

---

## ✅ Status

✔ Ready for XGBoost
✔ Supports both classification and regression
✔ Production-ready feature table
