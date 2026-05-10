# MLB Strikeout Prediction & Betting EV System — Complete Guide

Predicts how many strikeouts a starting pitcher will record against a given lineup, then surfaces over/under bets with positive expected value at Australian sportsbook odds.

**Two predictions, layered:**
1. **Per plate-appearance:** for every hitter–pitcher matchup in a game, predict P(strikeout)
2. **Per pitcher-game:** sum those P(K) values across the lineup via Poisson-binomial aggregation, producing an expected total Ks and a full probability distribution over 0, 1, 2, … n Ks

The game-level distribution powers betting. Knowing P(K = 7) precisely also gives you P(K > 5.5), P(K > 6.5), etc. — exactly what sportsbooks price.

---

## Table of Contents

1. [The ML Models — What They Are and Why](#1-the-ml-models--what-they-are-and-why)
2. [How the Data Is Split (and Why)](#2-how-the-data-is-split-and-why)
3. [Project Directory Structure](#3-project-directory-structure)
4. [Initial Setup (one-time)](#4-initial-setup-one-time)
5. [The Three Pipelines: Training, Recalibration, and Live Scoring](#5-the-three-pipelines-training-recalibration-and-live-scoring)
6. [The Daily Run Order — Full System](#6-the-daily-run-order--full-system)
7. [What the Model Outputs](#7-what-the-model-outputs)
8. [What You Need to Update Manually](#8-what-you-need-to-update-manually)
9. [The Daily Betting Workflow](#9-the-daily-betting-workflow)
10. [Metrics Reference: What Good and Bad Look Like](#10-metrics-reference-what-good-and-bad-look-like)
11. [How to Identify Good Bets](#11-how-to-identify-good-bets)
12. [Handedness — Reading Platoon Matchups](#12-handedness--reading-platoon-matchups)
13. [Things to Watch Out For](#13-things-to-watch-out-for)
14. [Troubleshooting](#14-troubleshooting)
15. [Maintenance](#15-maintenance)
16. [What to Build Next](#16-what-to-build-next)
17. [Glossary](#17-glossary)

---

## 1. The ML Models — What They Are and Why

The system uses **three different machine learning algorithms** layered together, plus a calibration step. Each piece does a specific job.

### The big picture: how the predictions are built

```
   For every batter-vs-pitcher matchup in a game:

        ┌─────────────────────────────────────────┐
        │      ~218 features about the matchup    │
        │  (rolling averages, velocities, etc.)   │
        └─────────────────────────────────────────┘
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
        ┌───────┐    ┌────────┐    ┌─────────┐
        │XGBoost│    │LightGBM│    │ Logistic│
        │       │    │        │    │  Regr.  │
        └───┬───┘    └────┬───┘    └────┬────┘
            │             │             │
            ▼             ▼             ▼
       Calibration   Calibration   Calibration   ← isotonic regression
       (XGB cal.)    (LGBM cal.)   (LR cal.)        on each model
            │             │             │
            └─────────────┼─────────────┘
                          ▼
                 ┌──────────────────┐
                 │ Weighted Average │
                 │  XGB: 55%        │
                 │  LGBM: 25%       │
                 │  LogReg: 20%     │
                 └──────────────────┘
                          │
                          ▼
                 Final calibration
                 (on the ensemble itself)
                          │
                          ▼
                 P(strikeout) for this PA
                          │
                          ▼
            (sum across all batters in the lineup)
                          │
                          ▼
                 Expected total Ks for the game
                 + Probability for each over/under line
```

### Feature reduction (334 → 218)

The original feature set contained 334 columns. After cross-model importance analysis (XGBoost gain + LightGBM gain, normalised 0–100), 116 redundant or near-zero-importance columns were removed. Key groups removed:
- Redundant handedness flags (`is_same_side_matchup`, `hitter_is_lefty`, etc.) — `hitter_bats` and `pitcher_throws` already encode this directly
- Weak single-window stats where a better window existed (e.g. kept `hitter_avg_batting_avg_last_10`; dropped last_3 and last_5)
- Single-game noise stats (`hitter_prev_pa`, `hitter_prev_ab`, etc.)
- Exact duplicates (`pitcher_is_starter` duplicates `pitcher_gamesStarted`)

This reduced training time and improved generalisation without losing any meaningful signal.

### The three base models

#### **XGBoost** (55% of ensemble) — the workhorse

**What it is:** Gradient-boosted decision trees. Builds hundreds of small decision trees, each one fixing the mistakes of the trees before it.

**Why we use it:** XGBoost is the single best-performing algorithm on tabular MLB data in published research. It handles missing values natively (pitcher previous-game stats are NULL for the first appearance of a season). It also handles non-linear relationships well — e.g. "a fastball over 96 mph against a hitter with high whiff rate produces dramatically more Ks than the linear sum of those two factors would suggest".

**Current hyperparameters:**
- `learning_rate`: 0.02 (reduced from 0.05 — slower learning prevents premature early stopping)
- `max_depth`: 5 (reduced from 6 — shallower trees reduce overfitting on noisy per-PA data)
- `min_child_weight`: 20 (increased from 5 — critical with PA-count sample weights)
- `gamma`: 0.05 (new — minimum gain required before splitting a node)
- `colsample_bytree`: 0.70 (reduced from 0.85 — more feature diversity per tree)
- Up to 5,000 rounds with 100-round early stopping

**You'll see it in the logs as:** `Training XGBoost...` and `[xgb_calibrated] AUC=...`

#### **LightGBM** (25% of ensemble) — the diversity provider

**What it is:** Same family as XGBoost (gradient-boosted trees) but uses a different splitting strategy ("leaf-wise" instead of "level-wise").

**Why we use it:** Two trees of the same family but different building strategies make different mistakes. When XGBoost overfits to a quirk in the data, LightGBM often doesn't, and vice versa.

**Current hyperparameters:**
- `learning_rate`: 0.02 (reduced from 0.05)
- `num_leaves`: 31 (reduced from 63 — the main LGBM fix; 63 leaves + early stopping was producing high-variance trees that lost to logistic regression)
- `min_data_in_leaf`: 80 (increased from 50)
- `min_sum_hessian_in_leaf`: 20.0 (new — critical with PA-count weights; equivalent to `min_child_weight` in XGBoost)
- `feature_fraction`: 0.70 (reduced from 0.85)
- Up to 5,000 rounds with 100-round early stopping

**Weight reduced to 25%** (from 40%) because the previous LGBM configuration (63 leaves, early stopping at ~55 rounds) was performing below logistic regression. After retraining with the corrected params above, LGBM's weight can be reassessed.

**You'll see it in the logs as:** `Training LightGBM...` and `[lgbm_calibrated] AUC=...`

#### **Logistic Regression** (20% of ensemble) — the sanity check

**What it is:** A linear model. Each feature contributes a fixed amount to the probability of a strikeout, with no interactions.

**Why we use it:**
1. **Sanity check.** If the trees aren't beating logistic regression by a meaningful margin, something's broken.
2. **Ensemble diversity.** A linear model makes fundamentally different errors than tree models. Its weight (raised to 20% from 10%) adds robustness.

**You'll see it in the logs as:** `Training Logistic Regression baseline...` and `[logreg_calibrated] AUC=...`

### The calibration step (Isotonic Regression)

This isn't a base model — it's a wrapper applied **after** each base model and again on top of the ensemble.

**What it does:** Maps raw model outputs to true probabilities. If XGBoost outputs "0.30" but in reality only 25% of those PAs end in strikeouts, isotonic regression learns that mapping (0.30 → 0.25) and corrects every future prediction.

**Why this is the most important piece for betting:** Sportsbook prices are probabilities. If your model says 30% but reality is 25%, every "over 5.5 Ks at 2.10" bet you place is mispriced. Calibration is the difference between a model that scores well on AUC and one that actually makes money.

**In-season recalibration:** Calibrators are initially fitted on 2025 validation data. As 2026 games accumulate, running `python run_daily.py --recalibrate` weekly re-fits the calibrators on completed 2026 outcomes. See sections 5 and 15.

**Where you'll see it working:** The Brier score in your evaluation metrics. A value below 0.14 means the probabilities reflect reality well.

### Why ensemble three models instead of just using XGBoost?

1. **Calibration improves.** Averaging multiple models reduces variance and softens overconfident predictions — exactly what calibration needs.
2. **Failure modes diversify.** When a single model has a bad day, the ensemble dilutes the damage.
3. **It's almost free.** Adding two extra models adds ~2 minutes to training time. The Brier improvement is small (~0.002) but every basis point matters when betting real money.

### Why not other approaches?

- **Direct game-level regression** (predict total Ks straight from a pitcher-level feature row): less data per model, no per-batter granularity, can't generate over-line probabilities easily. The PA-then-aggregate path is strictly more expressive.
- **Poisson regression on game totals:** a reasonable alternative, but assumes Ks are Poisson-distributed — they're not. They're Poisson-binomial (each batter has a different P(K)). The aggregation step in `game_aggregation.py` computes the *exact* Poisson-binomial distribution, which is more accurate.
- **Neural nets / TabNet / MLPs:** generally tie or lose to XGBoost on tabular MLB data unless you have millions of rows and very rich raw Statcast inputs. Not worth the added complexity here.

### Quick reference: what each model contributes

| Model | Weight | Role |
|---|---|---|
| XGBoost | 55% | Primary predictor — strongest single model |
| LightGBM | 25% | Diversifier — catches XGB's mistakes |
| Logistic Regression | 20% | Sanity check + diversity from a different model class |
| Isotonic Regression | (calibration) | Corrects probabilities so they reflect reality |

---

## 2. How the Data Is Split (and Why)

The pipeline splits data by **season**, not randomly. Shuffling would cause temporal leakage — the model would see May 2024 data during training and June 2024 data during testing, implicitly learning about future conditions.

### The split

| Split | Seasons | What it's used for |
|---|---|---|
| **Train** | 2023, 2024 | Fit XGBoost, LightGBM, and Logistic Regression |
| **Validation** | 2025 | Early stopping, fit initial calibration, tune the ensemble |
| **Test** | 2026 | Final held-out evaluation — never touched during training |

Configured in `config.py`:
```python
TRAIN_SEASONS = [2023, 2024]
VALIDATION_SEASON = 2025
TEST_SEASON = 2026
```

- **2023 and 2024 for training:** Two complete seasons (~61K PAs) to learn pitcher-batter dynamics across many matchups, parks, and conditions.
- **2025 for validation:** A complete unseen season. Used for early stopping, calibration, and ensemble tuning.
- **2026 for test:** The season you actually bet on. Its accuracy is your honest real-world estimate.

### Rolling the windows forward

When 2026 wraps up, edit `config.py`:
```python
TRAIN_SEASONS = [2024, 2025]
VALIDATION_SEASON = 2026
TEST_SEASON = 2027
```

Why drop 2023? Old data hurts in baseball — pitching philosophy and league-wide K rates evolve every year.

---

## 3. Project Directory Structure

```
mlb/
└── src/
    └── models/
        └── model_v4/
            ├── -- TRAINING (run weekly) --
            ├── pipeline.py              ← MAIN TRAINING ENTRY — fits the model
            ├── config.py                ← all settings (table names, hyperparameters, season splits)
            ├── data_loader.py           ← reads from SQL Server, splits data by season
            ├── features.py              ← selects ~218 feature columns, encodes categoricals
            ├── models.py                ← XGBoost + LightGBM + Logistic Regression + isotonic calibration
            ├── evaluation.py            ← all model metrics
            ├── game_aggregation.py      ← turns per-PA predictions into pitcher-game totals
            ├── sql_loader.py            ← SQL Server connection helper
            │
            ├── -- LIVE SCORING (run daily) --
            ├── run_daily.py             ← MAIN DAILY ENTRY — runs fetch + score
            │                              (add --recalibrate weekly, see section 5)
            ├── fetch_probable_pitchers.py ← hits MLB Stats API, populates dim_probable_pitchers
            ├── score_future_games.py    ← scores tomorrow's games using trained artifacts
            │
            ├── sql/
            │   ├── compute_ev.sql                      ← EV calculator (run after entering odds)
            │   ├── cleanup_duplicates.sql              ← one-time tool for duplicate removal
            │   ├── create_handedness_view.sql          ← creates handedness-enriched source view (one-time)
            │   ├── create_future_matchups_view.sql     ← creates future-matchups view (re-run after changes)
            │   └── drop_for_handedness_migration.sql   ← one-time migration tool
            │
            ├── artifacts/               ← saved models (created by pipeline.py; don't edit)
            │   ├── xgb.json
            │   ├── lgbm.txt
            │   ├── logreg.pkl
            │   ├── calibrators.pkl      ← updated by --recalibrate without full retrain
            │   └── feature_cols.pkl
            │
            └── logs/
                └── mlb_pipeline.log
```

**Files you edit by hand:**
- `config.py` — once, to update season splits when a new season starts
- `compute_ev.sql` — only to set `@BIAS_KS` after running the diagnostic query (see section 11)

**Files you should never modify directly:**
- Anything in `artifacts/` (regenerated each pipeline run or recalibration)

---

## 4. Initial Setup (one-time)

### Step 1: Install Python packages
```bash
pip install pandas numpy scikit-learn xgboost lightgbm sqlalchemy pyodbc requests
```

### Step 2: Confirm SQL Server connection
Open `sql_loader.py` and verify:
```python
SERVER = "localhost"
DATABASE = "mlb"
DRIVER = "ODBC Driver 17 for SQL Server"
```

### Step 3: Confirm upstream tables exist
```sql
SELECT TOP 5 * FROM mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2;
SELECT TOP 5 * FROM mlb.dbo.mlb_schedule WHERE gameDate >= CAST(GETDATE() AS DATE);
SELECT TOP 5 * FROM mlb.dbo.dim_player;
```

### Step 4: Create the handedness-enriched view (one-time)
```sql
-- In SSMS, open sql/create_handedness_view.sql and press F5
```

### Step 5: Create the future matchups view
```sql
-- In SSMS, open sql/create_future_matchups_view.sql and press F5
```

**Note:** Re-run this view whenever `create_future_matchups_view.sql` changes. The current version assigns PA counts by lineup rank (3 PAs for spots 1-3, 2 PAs for spots 4-9 = 21 total), correcting the previous flat 27 that over-inflated predicted Ks by ~1-2 per game.

### Step 6: Train the model for the first time
```bash
cd C:\Users\andre\PycharmProjects\ajo\mlb\src\models\model_v4
python pipeline.py
```

Takes 10-20 minutes. Creates trained model artifacts in `./artifacts/`.

### Step 7: Verify live scoring
```bash
python run_daily.py
```

Check results:
```sql
SELECT TOP 10 pitcher_name, opponent_team_name, game_date,
       predicted_strikeouts, predicted_k_stddev
FROM mlb.dbo.fact_pitcher_game_strikeout_predictions
WHERE split_set = 'future'
ORDER BY game_date, predicted_strikeouts DESC;
```

Predicted strikeouts in the 3-8 range for starters means you're set up correctly.

---

## 5. The Three Pipelines: Training, Recalibration, and Live Scoring

### Training pipeline (`pipeline.py`) — runs weekly

Fits the model from scratch. Reads 2023-2024 historical completed games, trains XGBoost / LightGBM / Logistic Regression, calibrates them on 2025 validation data, and saves everything to `./artifacts/`.

```
fact_hitter_pitcher_matchup_with_handedness  (135K+ historical rows)
                  ↓
            pipeline.py
                  ↓
        ./artifacts/   (saved model + calibrators)
                  ↓
    SQL output tables (predictions for validation + test seasons)
```

**Run:** `python pipeline.py` — once a week. Re-running daily adds noise without value.

### Recalibration pipeline (`run_daily.py --recalibrate`) — runs weekly from week 3-4 onwards

Re-fits only the **isotonic calibrators** on completed 2026 games — without retraining XGBoost, LightGBM, or Logistic Regression. The base models stay the same; only the probability-to-reality mapping is updated.

```
Completed 2026 games in the matchup view
                  ↓
    recalibrate_on_current_season()
    (in pipeline.py, called via run_daily.py --recalibrate)
                  ↓
    ./artifacts/calibrators.pkl  (updated in place)
                  ↓
    Re-run score_future_games.py to apply updated calibration
```

**Why this matters:** Calibrators were initially fitted on 2025 validation data. As 2026 outcomes accumulate, re-fitting on current-season data corrects any drift between 2025 and 2026 conditions (pitch-clock changes, new rule changes, lineup trends).

**When to start:** After 3-4 weeks into the 2026 season (~5,000+ PA rows completed). Too early = noisy calibration. Too late = leaving a correction on the table.

**Run:**
```bash
python run_daily.py --recalibrate
```

This recalibrates then scores future games in one command. After it completes, the updated calibrators are used automatically for all future scoring runs.

**Frequency:** Once a week, on the same day as your full retrain (e.g. Sunday).

### Live scoring pipeline (`run_daily.py`) — runs daily

Uses the **already-trained** models from `./artifacts/` to score tomorrow's games. Does NOT retrain or recalibrate.

```
Step 1: fetch_probable_pitchers.py
    → Hits MLB Stats API for tomorrow's probable pitchers
    → Stores results in dim_probable_pitchers

Step 2: score_future_games.py
    → Loads trained models from ./artifacts/
    → Reads from fact_future_matchups view (builds synthetic
       feature rows for tomorrow's games using each hitter's most
       recent historical feature snapshot; top-3 lineup spots
       get 3 synthetic PAs, spots 4-9 get 2 = 21 PAs total)
    → Predicts P(K) per matchup → aggregates via Poisson-binomial
    → Writes to fact_pitcher_game_strikeout_predictions (split_set='future')
    → Writes pristine rows to fact_pitcher_strikeout_betting_ev
```

**Run:** `python run_daily.py` — every morning before placing bets.

### Key takeaway

The model itself only changes when you run `pipeline.py`. Recalibration (weekly) updates the probability correction layer without retraining. The daily score script applies both to new game contexts.

---

## 6. The Daily Run Order — Full System

### Phase A — Upstream data refresh (your existing pipelines)

These build the raw data and feature tables that everything else depends on. **Run these first, after games complete each night:**

```bash
python -m src.pipelines.mlb_player_pitching_gamelogs_gamePk
python -m src.pipelines.mlb_player_hitting_gamelogs_gamePk
python -m src.pipelines.py_player_hitting_logs
python -m src.pipelines.py_player_pitching_logs
python -m src.pipelines.mlb_hitter_lineup
python -m src.pipelines.mlb_hitter_appearance
python -m src.featuresv2.hitter_features_v2
python -m src.featuresv2.pitcher_features_v2
python -m src.featuresv2.hitter_features_appearances_v2
python -m src.featuresv2.hitter_pitcher_matchup_feature_v2
```

**Frequency:** daily, after games complete.

### Phase B — Schedule refresh

Whatever populates `mlb_schedule` needs to run daily — that's where future games come from.

### Phase C — Model training

```bash
cd C:\Users\andre\PycharmProjects\ajo\mlb\src\models\model_v4
python pipeline.py
```

**Frequency:** weekly (Sunday night recommended).

### Phase C2 — In-season recalibration (starts week 3-4 of season)

```bash
cd C:\Users\andre\PycharmProjects\ajo\mlb\src\models\model_v4
python run_daily.py --recalibrate
```

**Frequency:** weekly, same day as the full retrain. On weeks when you retrain: run `python pipeline.py` first, then `python run_daily.py --recalibrate`.

### Phase D — Live scoring (daily, without recalibration)

```bash
cd C:\Users\andre\PycharmProjects\ajo\mlb\src\models\model_v4
python run_daily.py
```

**Frequency:** daily, in the morning before you intend to bet.

### Phase E — Bet recommendations (pre-game)

In SSMS:
1. Pull tomorrow's slate from `fact_pitcher_strikeout_betting_ev`
2. Open Sportsbet, find each pitcher's K prop
3. UPDATE the EV table with `sportsbook`, `line`, `over_odds`, `under_odds`
4. F5 `sql/compute_ev.sql`
5. Read recommendations and place bets

### Phase F — Settlement (next day)

After games complete, run Phase A upstream pipelines. Then:
```sql
-- Populate actual_strikeouts from pitching gamelogs
UPDATE ev
SET ev.actual_strikeouts = g.strikeOuts
FROM mlb.dbo.fact_pitcher_strikeout_betting_ev ev
JOIN mlb.dbo.fact_player_pitching_gamelogs g
  ON g.player_id = ev.pitcher_id
 AND g.gamePk    = ev.gamePk
WHERE ev.actual_strikeouts IS NULL
  AND g.strikeOuts IS NOT NULL;
```

Then re-run `sql/compute_ev.sql` to populate `bet_result` (WIN / LOSS / PUSH).

### When to run what — quick reference

| Task | Frequency | Command |
|---|---|---|
| Refresh raw data + features (Phase A) | Daily, after games end | Your 10 existing pipeline commands |
| Refresh schedule (Phase B) | Daily, morning | Whatever populates `mlb_schedule` |
| Re-train model (Phase C) | **Weekly** | `python pipeline.py` |
| **Recalibrate on 2026 data (Phase C2)** | **Weekly** (from week 3-4) | `python run_daily.py --recalibrate` |
| Fetch probable pitchers + score (Phase D) | **Daily**, morning | `python run_daily.py` |
| Enter sportsbook odds (Phase E) | Pre-game | UPDATE statements in SSMS |
| Compute EV (Phase E) | Pre-game | F5 `sql/compute_ev.sql` |
| Settle bet results (Phase F) | Day after | UPDATE actuals + re-run `sql/compute_ev.sql` |

---

## 7. What the Model Outputs

### `fact_pitcher_game_strikeout_predictions` — your daily betting board

One row per starting pitcher per game.

| Column | Meaning |
|---|---|
| `gamePk` | MLB's unique game ID |
| `game_date` | Date of the game |
| `pitcher_name` | The starter you're betting on |
| `pitcher_throws` | Pitcher's throwing arm — 'L' or 'R' |
| `opponent_team_name` | The lineup they're facing |
| `opp_lhb_count` | Left-handed batter PAs in the synthetic lineup |
| `opp_rhb_count` | Right-handed batter PAs |
| `opp_switch_count` | Switch-hitter PAs |
| `opp_same_side_count` | PAs with same-handedness matchup (K-friendly) |
| `opp_opposite_side_count` | PAs with opposite-handedness matchup |
| `batters_faced_modeled` | Total PAs used in the Poisson-binomial sum (typically 18-24 for starters) |
| `predicted_strikeouts` | Model's expected total Ks |
| `predicted_k_stddev` | Uncertainty around the prediction (lower = more confident) |
| `most_likely_k` | Single most-probable K count |
| `most_likely_k_prob` | Probability of that exact count |
| `prob_over_3_5` through `prob_over_9_5` | P(pitcher records ≥ threshold Ks) |
| `actual_strikeouts` | What actually happened (NULL until game completes) |
| `split_set` | `validation` (2025), `test` (2026), or `future` (upcoming) |

**How `actual_strikeouts` gets populated:** When Phase A upstream pipelines run after games complete and `pipeline.py` re-runs weekly, it back-fills actuals from the source matchup view automatically via `update_ev_actuals()`. For immediate settlement without waiting for a full retrain, run the Phase F UPDATE above directly.

### `fact_pitcher_strikeout_betting_ev` — your EV worksheet

Pre-populated with every test-season and future pitcher-game. You manually fill in the sportsbook columns; the SQL script fills in the rest.

**Identification and model output (pipeline-filled):**

| Column | Meaning |
|---|---|
| `pitcher_name`, `game_date`, `opponent_team_name` | Identification |
| `pitcher_throws` | 'L' or 'R' |
| `opp_lhb_count`, `opp_rhb_count`, `opp_switch_count` | Lineup handedness composition |
| `opp_same_side_count`, `opp_opposite_side_count` | Platoon advantage counts |
| `predicted_strikeouts`, `predicted_k_stddev` | Model output |

**Historical context (pipeline-filled):**

| Column | Meaning |
|---|---|
| `games_2023` | Pitcher's distinct appearances in 2023 |
| `games_2024` | Pitcher's distinct appearances in 2024 |
| `games_2025` | Pitcher's distinct appearances in 2025 |
| `games_2026` | Pitcher's distinct appearances in 2026 (so far) |
| `avg_k_last_3` | Average strikeout count over last 3 starts |
| `avg_k_last_5` | Average strikeout count over last 5 starts |
| `avg_k_last_10` | Average strikeout count over last 10 starts |
| `weighted_k_per_bf_last_3` | PA-weighted K rate per batter faced, last 3 starts |
| `weighted_k_per_bf_last_5` | PA-weighted K rate per batter faced, last 5 starts |
| `weighted_k_per_bf_last_10` | PA-weighted K rate per batter faced, last 10 starts |
| `avg_strike_pct_last_3/5/10` | Average strike percentage over last 3/5/10 starts |
| `avg_pitches_per_inning_last_3/5/10` | Average pitches per inning over last 3/5/10 starts |

**Note on avg_k columns:** These come from `fact_pitcher_model_featuresv2` via the pitcher's most recent gamePk in that table. If `pitcher_features_v2` hasn't run recently, the values may reflect an older period. If avg_k looks inconsistent with recent game logs, run Phase A pipelines to refresh the feature table, then re-run `python run_daily.py`.

**Sportsbook entry (you fill in):**

| Column | Example |
|---|---|
| `sportsbook` | 'Sportsbet' |
| `line` | 5.5 |
| `over_odds` | 1.90 (decimal, Australian-style) |
| `under_odds` | 1.90 |

**EV calculations (compute_ev.sql fills in):**

| Column | Meaning |
|---|---|
| `model_prob_over` | Model's P(K > line) — bias-adjusted via `@BIAS_KS` |
| `model_prob_under` | 1 − model_prob_over |
| `implied_prob_over` | 1 / over_odds |
| `implied_prob_under` | 1 / under_odds |
| `edge_over` / `edge_under` | model_prob − implied_prob |
| `ev_over` / `ev_under` | Expected return on $1 bet |
| `recommended_side` | OVER, UNDER, or PASS |
| `kelly_fraction` | Quarter-Kelly stake suggestion |
| `actual_strikeouts` | Filled in after game completes |
| `bet_result` | WIN / LOSS / PUSH |

### `fact_pa_strikeout_predictions` — per plate-appearance detail

Granular per-batter predictions. Use for deep-dive analysis: which specific hitters in this lineup is the model most confident about?

### `fact_model_evaluation_metrics` — performance log

A new batch of rows each pipeline run. Watch these over time — if Brier or MAE start drifting upwards, the model needs retraining.

```sql
SELECT * FROM mlb.dbo.fact_model_evaluation_metrics
ORDER BY run_timestamp DESC;
```

---

## 8. What You Need to Update Manually

### Sportsbook odds (every game day)

For every pitcher you want to consider betting:

```sql
UPDATE mlb.dbo.fact_pitcher_strikeout_betting_ev
SET sportsbook = 'Sportsbet',
    line = 5.5,
    over_odds = 1.90,
    under_odds = 1.90
WHERE game_date = '2026-05-10'
  AND pitcher_name = 'Sandy Alcantara';
```

**Decimal odds only.** Australian sportsbooks already use this format. If checking a US book, convert: `decimal = (american / 100) + 1` for positive American odds.

After updating odds, run `compute_ev.sql` to populate the EV columns.

### Bet results (after games finish)

`compute_ev.sql` reads `actual_strikeouts` to produce `bet_result` — but it does not populate `actual_strikeouts` itself. Populate it first:

```sql
-- Direct from pitching gamelogs (most reliable)
UPDATE ev
SET ev.actual_strikeouts = g.strikeOuts
FROM mlb.dbo.fact_pitcher_strikeout_betting_ev ev
JOIN mlb.dbo.fact_player_pitching_gamelogs g
  ON g.player_id = ev.pitcher_id
 AND g.gamePk    = ev.gamePk
WHERE ev.actual_strikeouts IS NULL
  AND g.strikeOuts IS NOT NULL;
```

Then re-run `compute_ev.sql` — Step 7 fills in `bet_result` automatically.

Alternatively, after a full weekly retrain, `pipeline.py` calls `update_ev_actuals()` which back-fills both `actual_strikeouts` and `bet_result` in one step for all completed games.

### Season splits (once per year)

When a new season begins, edit `config.py`:

```python
TRAIN_SEASONS = [2024, 2025]
VALIDATION_SEASON = 2026
TEST_SEASON = 2027
```

---

## 9. The Daily Betting Workflow

Once you're comfortable, your daily routine is roughly **15–20 minutes**:

### Morning (5 minutes) — settle yesterday

1. Run Phase A upstream pipelines (to refresh gamelogs with last night's results)

2. Populate actual strikeouts for settled bets:
   ```sql
   UPDATE ev
   SET ev.actual_strikeouts = g.strikeOuts
   FROM mlb.dbo.fact_pitcher_strikeout_betting_ev ev
   JOIN mlb.dbo.fact_player_pitching_gamelogs g
     ON g.player_id = ev.pitcher_id
    AND g.gamePk    = ev.gamePk
   WHERE ev.actual_strikeouts IS NULL
     AND g.strikeOuts IS NOT NULL;
   ```

3. Run `compute_ev.sql` to mark WIN/LOSS/PUSH on yesterday's bets.

4. Glance at performance (see section 15 monthly query).

### Afternoon (10 minutes) — score today's games

1. Run `python run_daily.py` to fetch probable pitchers and score today's games.

2. Pull tonight's slate:
   ```sql
   SELECT pitcher_name, opponent_team_name, predicted_strikeouts,
          predicted_k_stddev, prob_over_5_5, prob_over_6_5,
          avg_k_last_5, weighted_k_per_bf_last_5,
          opp_same_side_count, opp_opposite_side_count
   FROM mlb.dbo.fact_pitcher_strikeout_betting_ev
   WHERE game_date = CAST(GETDATE() AS DATE)
     AND sportsbook IS NULL
   ORDER BY predicted_strikeouts DESC;
   ```

3. Open your sportsbook. For each interesting pitcher, write down the line and over/under odds.

4. Update the EV table (see section 8).

5. Run `compute_ev.sql`.

6. Read the recommendations:
   ```sql
   SELECT pitcher_name, opponent_team_name, line, predicted_strikeouts,
          predicted_k_stddev, model_prob_over, over_odds, edge_over, ev_over,
          recommended_side, kelly_fraction,
          avg_k_last_5, weighted_k_per_bf_last_5
   FROM mlb.dbo.fact_pitcher_strikeout_betting_ev
   WHERE recommended_side IN ('OVER', 'UNDER')
     AND game_date = CAST(GETDATE() AS DATE)
   ORDER BY ev_over DESC;
   ```

### Pre-game (5 minutes) — place bets

1. Walk through the pre-bet sequence (see section 11) for each recommendation.
2. Place each recommended bet at the suggested Kelly fraction.
3. **Cap any single bet at 5% of bankroll** even if Kelly says higher.
4. Check confirmed lineups before final placement — late scratches invalidate predictions.

---

## 10. Metrics Reference: What Good and Bad Look Like

### Per-PA classifier metrics

#### **ROC AUC** (`roc_auc`)

Measures how well the model ranks PAs likely to end in K above those unlikely to. AUC runs lower than typical binary classifiers because the target is fractional.

| Value | Verdict |
|---|---|
| < 0.55 | **Bad** — model isn't learning |
| 0.55 – 0.58 | **Acceptable** |
| 0.58 – 0.62 | **Good** |
| 0.62 – 0.66 | **Excellent** |
| > 0.70 | **Suspicious** — likely data leakage |

#### **Brier Score** (`brier`)

How calibrated the probabilities are. **Most important metric for betting.** Lower is better.

| Value | Verdict |
|---|---|
| > 0.20 | **Bad** |
| 0.16 – 0.20 | **Poor to Acceptable** |
| 0.14 – 0.16 | **Good** |
| 0.12 – 0.14 | **Excellent** |

#### **Log Loss** (`log_loss_val`)

Penalty for confidently wrong predictions. Lower is better.

| Value | Verdict |
|---|---|
| > 0.55 | **Bad** |
| 0.45 – 0.55 | **Acceptable** |
| 0.40 – 0.45 | **Good** |

### Game-level metrics

#### **MAE** (Mean Absolute Error)

Average miss in predicted total Ks per pitcher-game.

| Value | Verdict |
|---|---|
| > 2.0 | **Bad** |
| 1.4 – 2.0 | **Acceptable** |
| 1.0 – 1.4 | **Good to Excellent** |

#### **Bias**

Whether the model systematically over- or under-predicts. Should be near zero.

| Value | Verdict |
|---|---|
| -0.2 to +0.2 | **Good** |
| +0.2 to +0.5 | **Over-predicting** — adjust `@BIAS_KS` in compute_ev.sql |
| beyond ±0.5 | **Bad** — recalibrate immediately |

### How to read your results dashboard

```sql
SELECT split_set, metric_type, label, n,
       ROUND(roc_auc, 4) AS auc,
       ROUND(brier, 4) AS brier,
       ROUND(log_loss_val, 4) AS log_loss,
       ROUND(mae, 3) AS mae,
       ROUND(bias, 3) AS bias
FROM mlb.dbo.fact_model_evaluation_metrics
WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM mlb.dbo.fact_model_evaluation_metrics)
ORDER BY split_set, metric_type, label;
```

Look at the `test` row labelled `ensemble`. That's your real-world performance number.

---

## 11. How to Identify Good Bets

### The metrics you care about per-bet

#### **edge_over** / **edge_under**

| Value | Verdict |
|---|---|
| < 0.03 | **Pass** |
| 0.03 – 0.06 | **Marginal** |
| 0.06 – 0.10 | **Good** edge |
| 0.10 – 0.15 | **Strong** edge |
| > 0.25 | **Suspicious** — verify the line and lineup |

#### **ev_over** / **ev_under**

The script flags bets with **EV > 5%**. Values below 5% are marked PASS.

| Value | Verdict |
|---|---|
| < 0 | **Negative EV** — never bet |
| 0 – 0.05 | **Below threshold** — PASS |
| 0.05 – 0.12 | **Acceptable** EV |
| 0.12 – 0.20 | **Good** EV |
| > 0.20 | **Strong** EV |

#### **kelly_fraction**

Quarter-Kelly stake as a fraction of bankroll.

| Value | Suggested action |
|---|---|
| 0 | Don't bet |
| 0.01 – 0.02 | Small stake (1–2%) |
| 0.02 – 0.04 | Moderate stake (2–4%) |
| 0.04 – 0.05 | Large stake — script's practical cap |

**The 5% cap is non-negotiable.** Model uncertainty is real. Full Kelly is only optimal if your probability estimates are exactly right — they're not. Quarter-Kelly cuts variance dramatically while capturing most of the long-run growth.

#### **predicted_k_stddev** — the model's uncertainty signal

The standard deviation of the Poisson-binomial distribution computed from per-PA probabilities. It answers: how wide is the realistic range of outcomes around the predicted mean?

A pitcher with `predicted_strikeouts = 6.0` and `predicted_k_stddev = 1.5`:
- Realistic range: **4–8 Ks** (predicted ± 1 stddev = ~68% of outcomes)
- Very likely range: **3–9 Ks** (predicted ± 2 stddev = ~95% of outcomes)

Stddev is highest when the pitcher faces many batters and per-PA K rates are near 0.5. It is lowest when per-PA probabilities are extreme (near 0 or 1).

**Stddev ranges and stake adjustments:**

| Stddev | Verdict | Action |
|---|---|---|
| < 1.4 | Very tight — model is confident | Bet full Kelly |
| 1.4 – 1.8 | Typical healthy range | Bet full Kelly |
| 1.8 – 2.1 | Acceptable | Bet full Kelly |
| 2.1 – 2.4 | Wider variance | Reduce to 75% of Kelly |
| 2.4 – 2.7 | High variance | Halve the Kelly stake |
| > 2.7 | Model is genuinely uncertain | Skip the bet |

**The combined check — how far is the line from the prediction?**

Stddev is most damaging when the sportsbook line sits close to the predicted K total:

```
signal_ratio = (predicted_strikeouts - line) / predicted_k_stddev
```

For UNDER bets: `(line - predicted_strikeouts) / predicted_k_stddev`

| signal_ratio | Verdict |
|---|---|
| > 1.0 | **Strong** — even with variance, the bet side is comfortable |
| 0.5 – 1.0 | **Acceptable** — reasonable confidence |
| 0.2 – 0.5 | **Marginal** — one bad inning erases the edge |
| < 0.2 | **Noise** — the bet is near a coin flip; pass regardless of EV |

**Example:** `predicted = 6.5`, `line = 5.5`, `stddev = 1.6` → signal_ratio = (6.5−5.5)/1.6 = **0.625** (acceptable OVER).

### Using the recent form columns to validate a bet

| Column | What to look for |
|---|---|
| `avg_k_last_3` vs `line` | If avg Ks over last 3 starts is above the line, that's supporting context for OVER |
| `weighted_k_per_bf_last_5` | K rate × 21 PAs = rough implied Ks. If this is near `predicted_strikeouts`, the prediction is grounded in recent form |
| `avg_strike_pct_last_5` | Strike% above 0.65 is a strong K environment |
| `avg_pitches_per_inning_last_5` | High pitches/IP (>17) may mean deeper counts but fewer innings pitched |
| `games_2026` | < 4 starts means limited current-season data — treat the prediction with more uncertainty |

### What to look for when placing a bet — pre-bet sequence

**Step 1 — EV and edge (minimum requirements)**
- `ev_over` or `ev_under` ≥ 0.05
- `edge_over` or `edge_under` ≥ 0.06

**Step 2 — Confidence check (`predicted_k_stddev`)**
- Stddev < 2.2 is comfortable; 2.2–2.5 is caution; > 2.5 consider skipping
- Calculate `signal_ratio = (predicted_strikeouts - line) / predicted_k_stddev`
  - Below 0.3: pass even if EV looks good
  - Above 0.6: the gap is meaningful — stddev is less of a concern

**Step 3 — Recent form cross-check**
- `avg_k_last_5` vs `line`: above the line over 5 starts supports OVER
- `weighted_k_per_bf_last_5 × 21` ≈ implied Ks from recent rate; large gap vs `predicted_strikeouts` means the model expects a form reversal — add caution
- `avg_strike_pct_last_5` ≥ 0.65 supports K volume
- `games_2026` < 4: limited current-season data — treat prediction as less reliable

**Step 4 — Matchup check**
- `same_side_ratio = opp_same_side_count / (opp_same_side_count + opp_opposite_side_count)`
  - > 0.55: K-friendly lineup for the pitcher
  - < 0.40: lineup is angled against the pitcher's handedness — extra scrutiny for OVER

**Step 5 — Confirm odds are still current**
- Lines move in the hours before game time; re-confirm before placing
- If the line has moved against your bet direction, recalculate EV with the new odds

**Step 6 — Confirm the lineup**
- Late scratches (top-3 hitters especially) can meaningfully shift the prediction
- Check confirmed lineups 30–60 min before first pitch

**Stake sizing:**
- Start with `kelly_fraction × bankroll`
- Apply the stddev adjustment from the table above
- **Hard cap: 5% of bankroll per bet, 15% total across all bets in one day**

### Putting it together: a "good bet" checklist

- [ ] `recommended_side` is OVER or UNDER (not PASS)
- [ ] `edge_over` (or `edge_under`) ≥ 0.06
- [ ] `ev_over` (or `ev_under`) ≥ 0.05
- [ ] `predicted_k_stddev` < 2.2
- [ ] `signal_ratio` (predicted − line) / stddev > 0.3
- [ ] Recent form (`avg_k_last_5`, `weighted_k_per_bf_last_5`) supports the prediction direction
- [ ] Lineup is confirmed (no late scratches)
- [ ] Not exceeding 15% of bankroll across all bets today

### About the bias correction in compute_ev.sql (`@BIAS_KS`)

The model may over-predict total Ks due to:
1. **Model training bias** — if base models over-estimate per-PA K probability, this multiplies up to ~1-2 extra Ks per game
2. **Synthetic PA inflation** — the future matchups view assigns 21 total PAs per game. If the actual starter faces fewer batters, predictions will be proportionally high

`compute_ev.sql` has a `@BIAS_KS` variable at the top:

```sql
DECLARE @BIAS_KS FLOAT = 0.0;  -- tune from the diagnostic query
```

When set positive (e.g. 1.0), it shifts the effective line up when reading from the pre-computed probability table. With `@BIAS_KS = 1.0` and a line of 4.5, the script looks up `prob_over_5_5` instead of `prob_over_4_5`.

**Setting `@BIAS_KS` correctly:**

Run the diagnostic query at the bottom of `compute_ev.sql`:

```sql
SELECT g.split_set, COUNT(*) AS games,
       ROUND(AVG(g.predicted_strikeouts), 2) AS avg_predicted,
       ROUND(AVG(CAST(g.actual_strikeouts AS FLOAT)), 2) AS avg_actual,
       ROUND(AVG(g.predicted_strikeouts - g.actual_strikeouts), 2) AS avg_bias
FROM dbo.fact_pitcher_game_strikeout_predictions g
WHERE g.actual_strikeouts IS NOT NULL
GROUP BY g.split_set;
```

| Measured avg_bias | Set `@BIAS_KS` |
|---|---|
| 0.0 – 0.3 | 0.0 (no correction) |
| 0.3 – 0.7 | 0.5 |
| 0.7 – 1.2 | 1.0 |
| 1.2 – 1.7 | 1.5 |
| > 1.7 | 2.0 |

`@BIAS_KS` must be a multiple of 0.5 — the probability table only has columns at 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5.

After retraining with corrected hyperparameters, re-run the diagnostic — bias should be near zero and `@BIAS_KS` can return to 0.0.

---

## 12. Handedness — Reading Platoon Matchups

Same-side matchups (RHP vs RHB, LHP vs LHB) produce K rates 2-4 percentage points higher than opposite-side matchups.

### What handedness columns mean

| Column | Meaning |
|---|---|
| `pitcher_throws` | 'L' or 'R' |
| `opp_lhb_count` | Left-handed batter PAs in the lineup |
| `opp_rhb_count` | Right-handed batter PAs |
| `opp_switch_count` | Switch-hitter PAs |
| `opp_same_side_count` | PAs with same-handedness matchup (best for K) |
| `opp_opposite_side_count` | PAs with opposite-handedness matchup |

These are PA counts (not hitter counts) — a hitter facing the pitcher 3 times contributes 3 to the count.

### The platoon advantage diagnostic

```sql
SELECT
    pitcher_name,
    pitcher_throws,
    opponent_team_name,
    predicted_strikeouts,
    opp_same_side_count,
    opp_opposite_side_count,
    CAST(opp_same_side_count AS FLOAT) /
        NULLIF(opp_same_side_count + opp_opposite_side_count, 0)
        AS same_side_ratio
FROM mlb.dbo.fact_pitcher_game_strikeout_predictions
WHERE game_date = CAST(GETDATE() AS DATE)
ORDER BY same_side_ratio DESC;
```

| `same_side_ratio` | Interpretation |
|---|---|
| > 0.55 | **Strong platoon edge** — K-friendly lineup |
| 0.45 – 0.55 | **Mixed** — typical |
| < 0.45 | **Reverse platoon** — harder for K |

---

## 13. Things to Watch Out For

### All recommendations showing OVER

If nearly every game shows `recommended_side = OVER`, the model is over-predicting K totals. Two root causes:
1. **Model bias** — run the diagnostic in `compute_ev.sql` and increase `@BIAS_KS` if avg_bias > 0.7
2. **Lines are too conservative** — if the gap between `predicted_strikeouts` and `line` is consistently > 1.5 Ks, the model may be correct and sportsbooks are setting low lines

The permanent fix is retraining (`python pipeline.py`).

### All recommendations showing UNDER

This is normal and expected. Sportsbooks deliberately set K prop lines high because recreational bettors love betting OVER. The value side in K markets is structurally more often UNDER. Before acting, verify that the model prediction AND recent form (avg_k_last_5) both support the under — if the model predicts low but recent form is high, add caution.

### Calibration drift

If `Brier` starts climbing across pipeline runs (e.g. 0.137 → 0.152), re-train and run `--recalibrate`. Usually happens after major league-wide changes.

### Bias drift

If overall `bias` consistently exceeds +0.4 across runs, increase `@BIAS_KS` in `compute_ev.sql`. If bias is negative (model under-predicting), decrease `@BIAS_KS` toward 0.0.

### Suspicious AUC jumps

If AUC suddenly > 0.70 when previous runs were ~0.58, investigate immediately — a leakage column has likely crept into `LEAKAGE_COLS` in `config.py`.

### avg_k columns out of sync with game logs

If `avg_k_last_3/5/10` look inconsistently high or low compared to a pitcher's recent game logs, the `fact_pitcher_model_featuresv2` feature table is stale. Verify:
```sql
SELECT TOP 1 p.player_id, p.gamePk, g.game_date, p.avg_k_last_3
FROM mlb.dbo.fact_pitcher_model_featuresv2 p
JOIN mlb.dbo.mlb_schedule g ON g.gamePk = p.gamePk
WHERE CAST(p.player_id AS INT) = <pitcher_id>
ORDER BY p.gamePk DESC;
```
If `game_date` is old, run Phase A pipelines including `pitcher_features_v2`, then re-run `python run_daily.py`.

### Late lineup changes

The model uses the most recent lineup it saw. If a star sits at game time, the prediction is wrong. Always check confirmed lineups before placing significant bets.

### Sportsbook line movement

Lines move. The odds you saw at lunch might shift at game time after sharp money. Either bet early or skip if the line moves against you.

### The 5% bankroll cap

Never stake more than 5% of bankroll on one bet, regardless of Kelly. A single 50% Kelly bet that loses can wipe out months of grinding.

### Sample weights

The source view has some rows representing single PAs and some representing multi-PA aggregates (`hitter_plate_appearances > 1`). The `sample_weight` column scales their influence. The pipeline uses it automatically — no action needed, but be aware that raw row counts in the source data don't equal PA counts.

---

## 14. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'config'` | Running from wrong directory | `cd` into the `model_v4` folder first |
| `Login failed for user` | SQL Server auth | Check `sql_loader.py` connection settings |
| `recommended_side` is all PASS | No edges, OR odds not entered | Check `over_odds` is filled in; also check `@BIAS_KS` isn't too high |
| `recommended_side` is all OVER | Model over-predicting (bias) | Run diagnostic in `compute_ev.sql`; increase `@BIAS_KS` or retrain |
| Duplicate rows in prediction tables | Pipeline ran twice without cleanup | Run `sql/cleanup_duplicates.sql` once; pipeline auto-clears going forward |
| Duplicate rows in EV table (two rows same game) | `pipeline.py` re-ran — completed game inserted as new test-set row alongside protected bet row | Delete the NULL-sportsbook duplicate: `DELETE FROM ev WHERE gamePk=? AND pitcher_id=? AND sportsbook IS NULL` |
| AUC dropped below 0.55 | Source data issue | Check `LEAKAGE_COLS` and recent source view changes |
| AUC above 0.70 | Leakage | Investigate `LEAKAGE_COLS`; a post-game column probably leaked in |
| `--recalibrate` warns "only N rows" | Season is too young | Wait until 3-4 weeks in (5,000+ PA rows). Proceed with existing 2025-fitted calibrators |
| `--recalibrate` runs but predictions unchanged | `score_future_games.py` not re-run | Run `python run_daily.py --recalibrate` (includes re-scoring) |
| `score_future_games.py` says "no future matchups found" | MLB API hasn't published probable pitchers, or `fact_future_matchups` view is stale | Run `python fetch_probable_pitchers.py`; check `dim_probable_pitchers` has rows |
| Future predictions look far too high (predicted > 8 Ks routinely) | Old model artifacts still in use | Re-run `python pipeline.py` with updated config hyperparameters |
| `predicted_strikeouts` looks too low (< 3) for starters | Reliever included, or `fact_future_matchups` view not recreated | Re-run `sql/create_future_matchups_view.sql` in SSMS; filter `batters_faced_modeled >= 15` |
| `Invalid object name 'dbo.fact_pitcher_game_strikeout_predictions'` | Table was dropped | Re-run `python pipeline.py` — `ensure_output_tables()` recreates it |
| `model_prob_over` is NULL after running compute_ev.sql | Line is not a standard .5 boundary (e.g. 4.3, 3.4) | Only lines 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5 are supported |
| New EV table columns (avg_k_last_3 etc.) are all NULL | Table predates the ALTER TABLE additions | Run `pipeline.py` or `score_future_games.py` — `ensure_output_tables()` adds columns automatically |
| `actual_strikeouts` not updating when compute_ev.sql runs | compute_ev.sql only reads actuals — doesn't populate them | Run the Phase F UPDATE against `fact_player_pitching_gamelogs` first, then re-run compute_ev.sql |
| avg_k columns don't match pitcher's recent game logs | `fact_pitcher_model_featuresv2` is stale | Run Phase A pipelines including `pitcher_features_v2`, then re-run `python run_daily.py` |

---

## 15. Maintenance

### Daily
- Run upstream data pipelines (Phase A)
- Run `python run_daily.py` for live scoring
- Settle yesterday's bets (Phase F UPDATE + re-run compute_ev.sql)

### Weekly
1. **Re-train model:**
   ```bash
   python pipeline.py
   ```

2. **Re-calibrate on current-season data** (from week 3-4 of the season onwards):
   ```bash
   python run_daily.py --recalibrate
   ```
   Sunday cycle: `python pipeline.py` → `python run_daily.py --recalibrate`. Mon–Sat: `python run_daily.py`.

   **Start using `--recalibrate` when:**
   - At least 3-4 weeks into the 2026 season
   - `fact_hitter_pitcher_matchup_with_handedness` has 5,000+ rows with `season = 2026` and `y_k_rate IS NOT NULL`

3. **Check metrics dashboard** for drift:
   ```sql
   SELECT split_set, metric_type, label,
          ROUND(roc_auc, 4) AS auc, ROUND(brier, 4) AS brier,
          ROUND(mae, 3) AS mae, ROUND(bias, 3) AS bias
   FROM mlb.dbo.fact_model_evaluation_metrics
   WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM mlb.dbo.fact_model_evaluation_metrics)
   ORDER BY split_set, metric_type, label;
   ```

### Monthly
Review your bet log:
```sql
SELECT
    COUNT(*) AS total_bets,
    SUM(CASE WHEN bet_result = 'WIN' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN bet_result = 'LOSS' THEN 1 ELSE 0 END) AS losses,
    AVG(CASE WHEN bet_result = 'WIN' THEN 1.0 ELSE 0.0 END) AS hit_rate,
    SUM(CASE
            WHEN bet_result = 'WIN' AND recommended_side = 'OVER' THEN over_odds - 1
            WHEN bet_result = 'WIN' AND recommended_side = 'UNDER' THEN under_odds - 1
            WHEN bet_result = 'LOSS' THEN -1
            ELSE 0
        END) AS profit_per_unit_staked
FROM mlb.dbo.fact_pitcher_strikeout_betting_ev
WHERE bet_result IS NOT NULL;
```

- If hit_rate > 0.55 with > 50 bets, your edge is showing. Continue.
- If profit_per_unit_staked is negative after 100+ bets, re-train and re-evaluate `@BIAS_KS`.

### Every 4-6 weeks
**Re-tune `@BIAS_KS` in `compute_ev.sql`** using the diagnostic query at the bottom of that script:

```sql
SELECT
    CASE
        WHEN predicted_strikeouts < 4 THEN '1. Low (<4)'
        WHEN predicted_strikeouts < 6 THEN '2. Mid (4-6)'
        WHEN predicted_strikeouts < 8 THEN '3. High (6-8)'
        ELSE '4. Very High (8+)'
    END AS bucket,
    COUNT(*) AS n_games,
    ROUND(AVG(predicted_strikeouts), 2) AS avg_pred,
    ROUND(AVG(CAST(actual_strikeouts AS FLOAT)), 2) AS avg_actual,
    ROUND(AVG(predicted_strikeouts - CAST(actual_strikeouts AS FLOAT)), 2) AS bias
FROM mlb.dbo.fact_pitcher_game_strikeout_predictions
WHERE split_set = 'test'
  AND actual_strikeouts IS NOT NULL
  AND batters_faced_modeled >= 15
GROUP BY
    CASE
        WHEN predicted_strikeouts < 4 THEN '1. Low (<4)'
        WHEN predicted_strikeouts < 6 THEN '2. Mid (4-6)'
        WHEN predicted_strikeouts < 8 THEN '3. High (6-8)'
        ELSE '4. Very High (8+)'
    END;
```

### Per-season
- Roll season splits in `config.py`
- Drop the four output tables and re-run `pipeline.py` for a clean slate
- Reset `@BIAS_KS` to 0.0 and re-measure after a few weeks of the new season

---

## 16. What to Build Next

In priority order:

1. **Park factors** — strikeout rates vary noticeably by ballpark (high altitude, foul territory size, etc.). Adding `park_id` as a categorical feature or merging in pitcher-friendly factor coefficients would tighten predictions by ~0.1-0.2 Ks per game.

2. **Umpire** — strike zone size varies meaningfully across umpires. Statcast publishes per-umpire zone data and CSW (called strikes + whiffs) rates. A tight-zone umpire working a high-K pitcher is a genuine OVER signal.

3. **Weather** — wind direction and game-time temperature affect K rate slightly. Free APIs are available (e.g. Open-Meteo). Most useful for games in extreme conditions (wind chill below 45°F, strong wind blowing in).

4. **Closing line value tracking** — log the line you bet at and the closing line. If your bets consistently beat the close, you have real edge. If not, you're getting lucky. This is the gold-standard check for model validity.

5. **SHAP values** — per-prediction feature attribution would let you sanity-check why the model thinks a pitcher will dominate or get rocked. Worth running for high-stake picks to catch edge cases where a single feature is driving an unusual prediction.

6. **A reliever model** — currently we only score starters for game totals. Reliever K rates are different and could be modelled separately if you want to bet team-total Ks or first-5-innings markets.

---

## 17. Glossary

| Term | Meaning |
|---|---|
| PA | Plate Appearance — one batter facing one pitcher, ends in a strikeout, walk, hit, out, etc. |
| K | Strikeout |
| Calibration | When a model says 30%, the event happens 30% of the time |
| AUC | Area Under ROC Curve — model's ability to rank Ks above non-Ks |
| Brier score | Mean squared error of probabilities vs binary outcomes |
| EV | Expected Value — average return on a $1 bet over many trials |
| Edge | model_prob − implied_prob — how much better your probability estimate is than the book's |
| Kelly criterion | Bet-sizing formula maximising long-run bankroll growth |
| Quarter-Kelly | Using 25% of the full Kelly stake — standard practice given model uncertainty |
| Vig / juice | The sportsbook's built-in margin (1.90/1.90 implies ~52.6% each side, not 50%) |
| Poisson-binomial | Distribution of the sum of independent non-identical Bernoullis (= total Ks given each batter has a different P(K)) |
| Isotonic regression | A monotone function fitted to map raw model scores to calibrated probabilities |
| signal_ratio | (predicted_strikeouts − line) / predicted_k_stddev — measures whether the edge is meaningful relative to model uncertainty |
| Platoon advantage | The tendency for same-handedness matchups (RHP vs RHB, LHP vs LHB) to produce more strikeouts than opposite-side matchups |
| Split set | The season category of a row: `train` (2023-24), `validation` (2025), `test` (2026), `future` (upcoming games) |
| @BIAS_KS | Variable in compute_ev.sql that shifts the effective line lookup to correct for systematic model over-prediction |

---

## Final reminders

1. **You will lose 40–45% of individual bets even with positive EV.** Variance is brutal over short samples. Trust the system over hundreds of bets, not dozens.
2. **Quarter-Kelly is conservative deliberately.** Never override the 5% hard cap upward.
3. **Check confirmed lineups before placing bets.** A late scratch invalidates the prediction.
4. **Run `--recalibrate` weekly from week 3-4 of the season.** The calibrators fitted on 2025 data get stale; current-season data improves them.
5. **If all recommendations are OVER, check `@BIAS_KS` and run the diagnostic.** The PA count fix (21 total vs old 27) and retraining should largely eliminate this, but some residual bias may remain.
6. **If avg_k columns don't match game logs, the feature table is stale.** Re-run Phase A pipelines including `pitcher_features_v2`.
7. **Track everything.** Your bet log is the only ground truth. Believe the data, not your gut.

Good luck.
