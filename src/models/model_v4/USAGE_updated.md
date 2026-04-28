# MLB Strikeout Betting Model — Operator's Guide

A practical, day-to-day guide for running the model, reading its outputs, entering sportsbook odds, and identifying genuinely good bets.

This document is the companion to the technical README. **The technical README explains how the model works internally — this document explains how to use it.**

---

## Table of Contents

1. [The ML Models — What They Are and Why](#1-the-ml-models--what-they-are-and-why)
2. [How the Data Is Split (and Why)](#2-how-the-data-is-split-and-why)
3. [Project Directory Structure](#3-project-directory-structure)
4. [Initial Setup (one-time)](#4-initial-setup-one-time)
5. [The Two Pipelines: Training vs Live Scoring](#5-the-two-pipelines-training-vs-live-scoring)
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

---

## 1. The ML Models — What They Are and Why

The system uses **three different machine learning algorithms** layered together, plus a calibration step. Each piece does a specific job. Understanding what each one does will help you trust the predictions and spot when something's gone wrong.

### The big picture: how the predictions are built

```
   For every batter-vs-pitcher matchup in a game:
   
        ┌─────────────────────────────────────────┐
        │      327 features about the matchup     │
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
                 │  XGB: 50%        │
                 │  LGBM: 40%       │
                 │  LogReg: 10%     │
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

### The three base models

#### **XGBoost** (50% of ensemble) — the workhorse

**What it is:** Gradient-boosted decision trees. Builds hundreds of small decision trees, each one fixing the mistakes of the trees before it.

**Why we use it:** XGBoost is the single best-performing algorithm on tabular MLB data in published research. It handles missing values natively (which matters because your source view has lots of NULLs — pitcher previous-game stats are NULL for the first appearance of a season, for example). It also handles non-linear relationships beautifully (e.g. "a fastball over 96 mph against a hitter with high whiff rate produces dramatically more Ks than the linear sum of those two factors would suggest").

**What it specifically does in this pipeline:** Predicts P(strikeout) for one batter facing one pitcher, using all 327 features. Trained for up to 2,000 rounds with early stopping if validation loss stops improving for 100 rounds. Your last run stopped at round 81 (very fast — the model converged quickly).

**You'll see it in the logs as:** `Training XGBoost...` and `[xgb_calibrated] AUC=...`

#### **LightGBM** (40% of ensemble) — the diversity provider

**What it is:** Same family as XGBoost (gradient-boosted trees) but uses a different splitting strategy ("leaf-wise" instead of "level-wise"). Microsoft's competitor to XGBoost.

**Why we use it:** Two trees of the same family but different building strategies make different mistakes. When XGBoost overfits to a quirk in the data, LightGBM often doesn't, and vice versa. Averaging both reduces the chance that one model's specific blind spot costs you a bet.

**What it specifically does:** Same job as XGBoost — predicts P(strikeout) per PA — but with a different internal logic. Often trains slightly faster than XGBoost on big tables.

**You'll see it in the logs as:** `Training LightGBM...` and `[lgbm_calibrated] AUC=...`

#### **Logistic Regression** (10% of ensemble) — the sanity check

**What it is:** A linear model. It assumes each feature contributes a fixed amount to the probability of a strikeout, with no interactions.

**Why we use it (despite being weaker):** Two reasons:
1. **Sanity check.** If the trees aren't beating logistic regression by a meaningful margin, something's broken. They should always win, because Ks involve interactions (batter handedness × pitcher pitch mix, count situation × velocity, etc.) that linear models can't capture.
2. **Ensemble diversity.** A linear model makes fundamentally different errors than tree models. Its small weight in the ensemble adds robustness without hurting accuracy when the tree models agree.

**What it specifically does:** Same prediction task, but as a linear weighted sum of the features (after standardisation). Much weaker as a standalone model — but useful as a diversifier.

**You'll see it in the logs as:** `Training Logistic Regression baseline...` and `[logreg_calibrated] AUC=...`

### The calibration step (Isotonic Regression)

This isn't a base model — it's a wrapper applied **after** each base model and again on top of the ensemble.

**What it does:** Maps raw model outputs to true probabilities. If XGBoost outputs "0.30" but in reality only 25% of those PAs actually end in strikeouts, isotonic regression learns that mapping (0.30 → 0.25) and corrects every future prediction.

**Why this is the most important piece for betting:** Sportsbook prices are probabilities. If your model says 30% but reality is 25%, every "over 5.5 Ks at 2.10" bet you place based on the raw 30% is mispriced. Calibration is the difference between a model that scores well on AUC and a model that actually makes money.

**Where you'll see it working:** The Brier score in your evaluation metrics. A value below 0.14 means the probabilities reflect reality well — exactly what betting needs.

### Why ensemble three models instead of just using XGBoost?

Three reasons:

1. **Calibration improves.** Averaging multiple models reduces variance and softens overconfident predictions, which is exactly what calibration needs to work well.
2. **Failure modes diversify.** When a single model has a bad day (because of a weird data point or a feature drift), the ensemble dilutes the damage.
3. **It's almost free.** Adding two extra models added maybe 2 minutes to training time. The Brier improvement from ensembling vs. solo XGBoost is small (~0.002 in absolute terms), but every basis point of calibration matters when you're betting real money.

### Other algorithms I considered and rejected

You might wonder why I didn't use these:

| Algorithm | Why I didn't use it |
|---|---|
| **Random Forest** | Strictly weaker than gradient boosting on tabular data. No reason to use it when we have XGBoost. |
| **Neural networks / TabNet** | Need millions of rows to beat XGBoost on tabular data. With ~135K rows, NNs lose. |
| **CatBoost** | Excellent algorithm, would work fine here. Not used because XGBoost + LightGBM already cover the gradient-boosted-trees space; adding CatBoost gives marginal improvement at the cost of a third dependency. |
| **Poisson regression** (direct game-level) | Models total Ks as Poisson-distributed. But Ks aren't Poisson — they're Poisson-binomial (each batter has a different P(K)). The pipeline already does the exact Poisson-binomial calculation in the aggregation step, which is more accurate. |
| **Direct game-level XGBoost** | Predicting total Ks straight from a pitcher-game row. Wastes the per-batter granularity in your data. The PA-then-aggregate path is strictly more expressive. |

### How the target variable works (important detail)

Your source view has **one row per hitter**, not one row per plate appearance. A hitter who faces the pitcher 3 times in a game is one row with `hitter_plate_appearances = 3`. Two technical consequences:

1. **The model's target is a per-PA strikeout *rate*** (a fraction between 0 and 1), not a binary 0/1. A hitter who struck out 1 time in 3 PAs has target = 0.333. The model learns this fractional rate directly, weighted by the number of PAs that row represents.

2. **Sample weights matter.** A row representing 3 PAs counts 3× as much in the loss as a row representing 1 PA. This is why isotonic calibration in this pipeline accepts `sample_weight` — without it, hitters with few PAs would dominate.

**Why you care as an operator:** the `predicted_strikeouts` column you read each day is the result of summing per-PA probabilities across the entire lineup. If the source view ever stops populating `hitter_plate_appearances` correctly, your predictions will be off by a constant factor. The diagnostic queries in section 10 will catch this — average predicted Ks will be way off from average actual Ks.

### Quick reference: what each model contributes to your final prediction

| Model | Weight | Role |
|---|---|---|
| XGBoost | 50% | Primary predictor — strongest single model |
| LightGBM | 40% | Diversifier within the same family — catches XGB's mistakes |
| Logistic Regression | 10% | Sanity check + extra diversity from a different model class |
| Isotonic Regression | (calibration) | Corrects probabilities so they reflect reality |

---

## 2. How the Data Is Split (and Why)

The pipeline splits your data into three groups by **season**, not randomly. This matters a lot. Here's the full reasoning.

### The split

| Split | Seasons | What it's used for |
|---|---|---|
| **Train** | 2023, 2024 | Fit XGBoost, LightGBM, and Logistic Regression |
| **Validation** | 2025 | Decide when to stop training, fit calibration, tune the ensemble |
| **Test** | 2026 | Final held-out evaluation — never touched during training |

Configured in `config.py`:
```python
TRAIN_SEASONS = [2023, 2024]
VALIDATION_SEASON = 2025
TEST_SEASON = 2026
```

Your last run produced these counts:
- Train: 61,522 PAs (2023–24)
- Validation: 57,267 PAs (2025)
- Test: 16,792 PAs (2026 — incomplete, since the season is ongoing)

### Why split by season instead of randomly?

**This is the single most important design decision in the pipeline.** It deserves an explanation.

If you randomly shuffled all 135,581 rows and put 70% in train and 30% in test, the model would see PAs from May 2024 in training **and** PAs from June 2024 in test. That seems harmless — but it isn't.

Here's why it matters:

1. **Player attributes change within a season.** A pitcher who learns a new slider in April 2024 is a different pitcher than the same pitcher in March 2024. Random splitting lets the model "learn" what that pitcher will do later in the same season — which is exactly what you'd want it *not* to do.

2. **League-wide trends change.** Strike zones change. Ball composition changes. The pitch clock was introduced in 2023. A model that trains on 2023 mid-season and tests on 2023 late-season has implicitly learned the early-season strike zone and gets credit for it on late-season data — but in real betting, you only ever predict *future* games.

3. **You only ever bet on future games.** The whole point of evaluating a model is to estimate how it'll perform on data it hasn't seen yet. Random splitting overestimates that performance because the model has effectively peeked at data from the same time period.

The technical term for this is **temporal leakage**. It's the most common reason models that look great in development lose money in production.

### Why these specific seasons?

- **2023 and 2024 for training:** Two complete seasons gives the model enough data (~61K PAs) to learn pitcher-batter dynamics across many different matchups, parks, and weather conditions.
- **2025 for validation:** A complete season the model hasn't seen. Used for early stopping, calibration, and ensemble tuning. Big enough (~57K PAs) for those decisions to be statistically reliable.
- **2026 (current) for test:** This is the season you actually want to bet on. The model has never seen any of it — so its accuracy on this split is your honest estimate of real-world performance.

### What this means for your daily workflow

When you're looking at predictions in `fact_pitcher_game_strikeout_predictions`:

- Rows where `split_set = 'test'` are the ones for **today and going forward** — these are the predictions you bet on.
- Rows where `split_set = 'validation'` (the 2025 season) are useful for sanity checks and confidence-building — you can compare predicted vs actual K totals to see how often the model was right.

### Why the validation set exists separately from the test set

People sometimes ask: "Why don't you just train on 2023+2024+2025 and use 2026 as test?"

Two reasons you need a separate validation set:

1. **Early stopping.** XGBoost and LightGBM both stop training when the validation loss stops improving. The validation set must be different from training (so you're not over-fitting) AND different from test (so you're not contaminating your real-world estimate).

2. **Calibration.** Isotonic regression needs a separate dataset to learn the probability-to-reality mapping. If you fit isotonic regression on the training set, it learns to "correct" the over-confidence the trees show on training data — which doesn't help on new data. Fitting it on the validation set teaches it to correct the over-confidence it shows on data it hasn't been trained on, which is exactly what we want.

If you don't have a validation set, you have to choose between optimal early stopping and good calibration. Having three sets lets you have both.

### Rolling the windows forward

When 2026 wraps up, you'll edit `config.py`:

```python
TRAIN_SEASONS = [2024, 2025]      # add 2025 to training, drop 2023
VALIDATION_SEASON = 2026          # 2026 becomes validation
TEST_SEASON = 2027                # next season is your test set
```

Why drop 2023? Old data hurts in baseball. Pitching philosophy, hitter approaches, and league-wide K rates evolve every year. Training on the most recent two seasons gives you a model that reflects current MLB. If you trained on 2019–2025, the model would be partially fitted to a league that no longer exists.

Mid-season retraining (every 1-2 weeks) doesn't change the splits — it just picks up newly-recorded games into whichever season they belong to.

### Common questions

**"Why not use 5-fold cross-validation?"**
Because folds need to be time-ordered too. Standard k-fold cross-validation randomly shuffles, which causes the temporal leakage problem above. Time-ordered cross-validation is fancier than we need here — three time-ordered splits do the job.

**"What if I want to train on more data?"**
You can. Edit `TRAIN_SEASONS = [2022, 2023, 2024]` if you want to include 2022. Just be aware that older data may hurt more than help in baseball. Test the model with and without the extra year to see.

**"Should I retrain after every game day?"**
No. Retraining adds noise — early stopping picks slightly different cut-off points each run, calibration shifts slightly, etc. Once a week or two is plenty. The model isn't sensitive to whether you've added 50 more games of data overnight.

---

## 3. Project Directory Structure

Your project lives in `C:\Users\andre\PycharmProjects\ajo\mlb\src\models\model_v4\`. Here's what each file does:

```
mlb/
└── src/
    └── models/
        └── model_v4/
            ├── __init__.py              ← marks this folder as a Python package
            │
            ├── -- TRAINING (run weekly) --
            ├── pipeline.py              ← MAIN TRAINING ENTRY — fits the model
            ├── config.py                ← all settings (table names, hyperparameters, season splits)
            ├── data_loader.py           ← reads from SQL Server, splits data by season
            ├── features.py              ← selects feature columns, encodes categoricals
            ├── models.py                ← XGBoost + LightGBM + Logistic Regression + isotonic calibration
            ├── evaluation.py            ← all model metrics
            ├── game_aggregation.py      ← turns per-PA predictions into pitcher-game totals
            ├── sql_loader.py            ← your SQL Server connection helper
            │
            ├── -- LIVE SCORING (run daily) --
            ├── run_daily.py             ← MAIN DAILY ENTRY — runs both fetch + score
            ├── fetch_probable_pitchers.py ← hits MLB Stats API, populates dim_probable_pitchers
            ├── score_future_games.py    ← scores tomorrow's games using trained artifacts
            │
            ├── sql/
            │   ├── compute_ev.sql                      ← EV calculator (run after entering odds)
            │   ├── cleanup_duplicates.sql              ← one-time tool for duplicate removal
            │   ├── create_handedness_view.sql          ← creates handedness-enriched source view (one-time)
            │   ├── create_future_matchups_view.sql     ← creates future-matchups view (one-time)
            │   └── drop_for_handedness_migration.sql   ← one-time migration tool
            │
            ├── artifacts/               ← saved models (created by pipeline.py; don't edit)
            │   ├── xgb.json
            │   ├── lgbm.txt
            │   ├── logreg.pkl
            │   ├── calibrators.pkl
            │   └── feature_cols.pkl
            │
            └── logs/                    ← run logs (created by pipeline)
                └── mlb_pipeline.log
```

**Files you edit by hand:**
- `config.py` — once, to update season splits when a new season starts
- `compute_ev.sql` — never (just run it)

**Files you should never modify:**
- Anything in `artifacts/` (it's regenerated each run)
- The other `.py` files unless you know what you're doing

**Files generated automatically:**
- Everything in `artifacts/` and `logs/`
- The four SQL Server tables (covered in section 7)

---

## 4. Initial Setup (one-time)

You've already done some of this. For reference / future you, here's the complete setup:

### Step 1: Install Python packages
```bash
pip install pandas numpy scikit-learn xgboost lightgbm sqlalchemy pyodbc requests
```

(`requests` is needed for the MLB Stats API calls in `fetch_probable_pitchers.py`.)

### Step 2: Confirm SQL Server connection
Open `sql_loader.py` and verify:
```python
SERVER = "localhost"
DATABASE = "mlb"
DRIVER = "ODBC Driver 17 for SQL Server"
```

### Step 3: Confirm upstream tables exist
The model depends on these tables being populated by your upstream pipelines:
- `fact_hitter_pitcher_matchup_model_featuresv2` (training data)
- `mlb_schedule` (game schedule, including future games)
- `dim_player` (player attributes including handedness)
- `fact_player_hit_statcast` (raw Statcast data)

In SSMS:
```sql
SELECT TOP 5 * FROM mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2;
SELECT TOP 5 * FROM mlb.dbo.mlb_schedule WHERE gameDate >= CAST(GETDATE() AS DATE);
SELECT TOP 5 * FROM mlb.dbo.dim_player;
```

If any of these are empty, run your upstream pipelines first (see section 6).

### Step 4: Create the handedness-enriched view (one-time)
```sql
-- In SSMS, open sql/create_handedness_view.sql and press F5
```

Verify:
```sql
SELECT TOP 5 hitter_bats, pitcher_throws, platoon_matchup
FROM mlb.dbo.fact_hitter_pitcher_matchup_with_handedness;
```

You should see 'L'/'R'/'S' values populating.

### Step 5: Train the model for the first time
```bash
cd C:\Users\andre\PycharmProjects\ajo\mlb\src\models\model_v4
python pipeline.py
```

This takes 8-15 minutes and creates the trained model artifacts in `./artifacts/`. After it completes, four output tables exist in SQL Server.

### Step 6: Set up live scoring infrastructure
Run these once to set up future-game scoring:

```bash
# Test the probable pitcher API access
python fetch_probable_pitchers.py
```

This should write rows to `mlb.dbo.dim_probable_pitchers`. Then in SSMS:

```sql
-- Open and F5: sql/create_future_matchups_view.sql
```

This creates the `fact_future_matchups` view that combines tomorrow's games with the latest hitter and pitcher feature snapshots.

### Step 7: Verify the live scoring works end-to-end
```bash
python score_future_games.py
```

Should produce predictions for tomorrow's games. Verify:
```sql
SELECT TOP 10 pitcher_name, opponent_team_name, game_date,
       predicted_strikeouts, predicted_k_stddev
FROM mlb.dbo.fact_pitcher_game_strikeout_predictions
WHERE split_set = 'future'
ORDER BY game_date, predicted_strikeouts DESC;
```

If predicted strikeouts are in the 3-9 range for starters, you're set up correctly.

---

## 5. The Two Pipelines: Training vs Live Scoring

The system has **two separate Python pipelines** that work together. Understanding the difference is critical.

### Training pipeline (`pipeline.py`) — runs weekly

This is what fits the model. It reads historical completed games, trains XGBoost / LightGBM / Logistic Regression, calibrates them, and saves the trained models to disk in `./artifacts/`.

```
fact_hitter_pitcher_matchup_with_handedness  (135K+ historical rows)
                  ↓
            pipeline.py
                  ↓
        ./artifacts/   (saved models)
                  ↓
    SQL output tables (predictions for validation + test seasons)
```

**Run:** `python pipeline.py` — once a week is plenty. Re-running daily adds noise without value.

### Live scoring pipeline (`run_daily.py`) — runs daily

This uses the **already-trained** models from `./artifacts/` to score tomorrow's games. It does NOT re-train. Two steps:

```
Step 1: fetch_probable_pitchers.py
    → Hits MLB Stats API for tomorrow's probable pitchers
    → Stores results in dim_probable_pitchers

Step 2: score_future_games.py
    → Loads trained models from ./artifacts/
    → Reads from fact_future_matchups view (which builds synthetic
       feature rows for tomorrow's games using each hitter's most
       recent historical feature snapshot)
    → Predicts P(K) per matchup → aggregates to pitcher-game
    → Writes to fact_pitcher_game_strikeout_predictions (split_set='future')
    → Writes pristine rows to fact_pitcher_strikeout_betting_ev
```

**Run:** `python run_daily.py` — every morning before placing bets.

### Key takeaway

The model itself only changes when you run `pipeline.py`. The daily score script just applies the existing model to new game contexts. That's why daily runs are fast (under a minute) compared to training runs (8-15 minutes).

---

## 6. The Daily Run Order — Full System

Your full pipeline involves three layers: upstream data refresh, model training, and live scoring. Here's the complete order:

### Phase A — Upstream data refresh (your existing pipelines)

These build the raw data and feature tables that everything else depends on. **Run these first**, in this exact order, because each builds on the previous:

```bash
# Game logs (raw box scores) - foundational completed-game data
python -m src.pipelines.mlb_player_pitching_gamelogs_gamePk
python -m src.pipelines.mlb_player_hitting_gamelogs_gamePk

# Statcast pitch-by-pitch data
python -m src.pipelines.py_player_hitting_logs
python -m src.pipelines.py_player_pitching_logs

# Lineup data
python -m src.pipelines.mlb_hitter_lineup
python -m src.pipelines.mlb_hitter_appearance

# Engineered feature tables
python -m src.featuresv2.hitter_features_v2
python -m src.featuresv2.pitcher_features_v2
python -m src.featuresv2.hitter_features_appearances_v2
python -m src.featuresv2.hitter_pitcher_matchup_feature_v2
```

This populates `fact_hitter_pitcher_matchup_model_featuresv2`. The handedness view (`fact_hitter_pitcher_matchup_with_handedness`) sits on top of it, so it picks up new data automatically.

**Frequency:** daily, after games complete. These pipelines load completed games only.

### Phase B — Schedule refresh

Whatever populates `mlb_schedule` needs to run too — that's where future games come from. Confirm what loads it (probably another upstream pipeline) and run it daily.

### Phase C — Model training

```bash
cd C:\Users\andre\PycharmProjects\ajo\mlb\src\models\model_v4
python pipeline.py
```

**Frequency:** weekly. Sunday night is a good time. Re-training picks up the latest data into the trained models.

### Phase D — Live scoring (daily)

```bash
cd C:\Users\andre\PycharmProjects\ajo\mlb\src\models\model_v4
python run_daily.py
```

This does:
1. Fetches tomorrow's probable pitchers from the MLB Stats API
2. Scores those games using the trained model artifacts
3. Writes predictions to SQL Server

**Frequency:** daily, in the morning before you intend to bet.

### Phase E — Bet recommendations (pre-game)

In SSMS:

1. Pull tomorrow's slate from `fact_pitcher_strikeout_betting_ev`
2. Open Sportsbet, find each pitcher's K prop
3. UPDATE the EV table with `sportsbook`, `line`, `over_odds`, `under_odds`
4. F5 `sql/compute_ev.sql`
5. Read recommendations and place bets

### Phase F — Settlement (next day)

After games complete, your upstream pipelines (Phase A) re-populate `actual_strikeouts` as the data comes in. Then re-run `sql/compute_ev.sql` to populate `bet_result` (WIN / LOSS / PUSH).

### When to run what — quick reference

| Task | Frequency | Command |
|---|---|---|
| Refresh raw data + features (Phase A) | Daily, after games end | Your 10 existing pipeline commands |
| Refresh schedule (Phase B) | Daily, morning | Whatever populates `mlb_schedule` |
| Re-train model (Phase C) | **Weekly** | `python pipeline.py` |
| Fetch probable pitchers + score (Phase D) | **Daily**, morning | `python run_daily.py` |
| Enter sportsbook odds (Phase E) | Pre-game | UPDATE statements in SSMS |
| Compute EV (Phase E) | Pre-game | F5 `sql/compute_ev.sql` |
| Settle bet results (Phase F) | Day after | Re-run `sql/compute_ev.sql` |

### Why not re-train daily?

Re-training shifts feature weights and isotonic calibration slightly each run. The bias correction values in `compute_ev.sql` were calibrated against a specific set of model artifacts. Re-training daily creates noise and would technically require re-calibrating the bias correction each time. **Once a week** captures real data improvements while keeping the bias correction valid.

---

## 7. What the Model Outputs

The pipeline writes four tables to `mlb.dbo`. Here's what each holds and when you use it.

### 6.1 `fact_pitcher_game_strikeout_predictions` — your daily betting board

**This is the table you'll use most.** One row per starting pitcher per game.

| Column | Meaning |
|---|---|
| `gamePk` | MLB's unique game ID |
| `game_date` | Date of the game |
| `pitcher_name` | The starter you're betting on |
| `pitcher_throws` | Pitcher's throwing arm — 'L' or 'R' |
| `opponent_team_name` | The lineup they're facing |
| `opp_lhb_count` | Total left-handed batter PAs in the lineup |
| `opp_rhb_count` | Total right-handed batter PAs |
| `opp_switch_count` | Total switch-hitter PAs |
| `opp_same_side_count` | PAs with same-handedness matchup (K-friendly) |
| `opp_opposite_side_count` | PAs with opposite-handedness matchup |
| `batters_faced_modeled` | How many batters were factored into the prediction (typically 18–28 for starters) |
| `predicted_strikeouts` | Model's expected total Ks |
| `predicted_k_stddev` | Uncertainty around the prediction (lower = more confident) |
| `most_likely_k` | Single most-probable K count |
| `most_likely_k_prob` | Probability of that exact count |
| `prob_over_3_5` | P(pitcher records ≥ 4 Ks) |
| `prob_over_4_5` | P(≥ 5 Ks) |
| `prob_over_5_5` | P(≥ 6 Ks) — most common betting line |
| `prob_over_6_5` | P(≥ 7 Ks) |
| `prob_over_7_5` | P(≥ 8 Ks) |
| `prob_over_8_5` | P(≥ 9 Ks) |
| `prob_over_9_5` | P(≥ 10 Ks) |
| `actual_strikeouts` | What actually happened (NULL until game completes) |
| `split_set` | `validation` (2025) or `test` (2026) |

For more on how to read the handedness columns, see section 12.

### 6.2 `fact_pitcher_strikeout_betting_ev` — your EV worksheet

Pre-populated with every test-season pitcher-game. **You manually fill in the sportsbook columns**, then a SQL script fills in the rest.

| Column | Filled by | Purpose |
|---|---|---|
| `pitcher_name`, `game_date`, `opponent_team_name` | Pipeline | Identification |
| `predicted_strikeouts`, `predicted_k_stddev` | Pipeline | Model output |
| `sportsbook` | **YOU** | e.g. 'Sportsbet', 'Bet365' |
| `line` | **YOU** | e.g. 5.5 |
| `over_odds` | **YOU** | Decimal odds, e.g. 1.90 |
| `under_odds` | **YOU** | Decimal odds, e.g. 1.90 |
| `model_prob_over` | EV script | Model's P(K > line) |
| `implied_prob_over` | EV script | 1 / over_odds |
| `edge_over` | EV script | model_prob_over − implied_prob_over |
| `ev_over` | EV script | Expected return on $1 OVER bet |
| `recommended_side` | EV script | OVER, UNDER, or PASS |
| `kelly_fraction` | EV script | Quarter-Kelly stake suggestion |
| `bet_result` | EV script | WIN / LOSS / PUSH (after game) |

### 6.3 `fact_pa_strikeout_predictions` — per plate-appearance detail

Granular per-batter predictions. You usually won't query this directly. Use it for deep-dive analysis like "which specific hitters in this lineup is the model most confident about?"

### 6.4 `fact_model_evaluation_metrics` — performance log

A new batch of rows each pipeline run. Lets you watch the model's accuracy over time.

```sql
SELECT * FROM mlb.dbo.fact_model_evaluation_metrics
ORDER BY run_timestamp DESC;
```

---

## 8. What You Need to Update Manually

Three things, in order of frequency:

### 7.1 Sportsbook odds (every game day)

This is the only regular manual work. For every pitcher you want to consider betting:

```sql
UPDATE mlb.dbo.fact_pitcher_strikeout_betting_ev
SET sportsbook = 'Sportsbet',
    line = 5.5,
    over_odds = 1.90,
    under_odds = 1.90
WHERE gamePk = 824932
  AND pitcher_id = 691725;
```

**Decimal odds only.** Australian sportsbooks already use this format. If you're checking a US book, convert: `decimal = (american / 100) + 1` for positive American odds.

After updating odds, run `compute_ev.sql` to populate the EV columns.

### 7.2 Bet results (after games finish)

The pipeline pulls `actual_strikeouts` automatically when you re-run it. Or update directly:

```sql
UPDATE ev
SET ev.actual_strikeouts = g.actual_strikeouts
FROM mlb.dbo.fact_pitcher_strikeout_betting_ev ev
JOIN mlb.dbo.fact_pitcher_game_strikeout_predictions g
  ON g.gamePk = ev.gamePk AND g.pitcher_id = ev.pitcher_id
WHERE ev.actual_strikeouts IS NULL
  AND g.actual_strikeouts IS NOT NULL;
```

Then re-run `compute_ev.sql` — it'll fill in `bet_result` automatically.

### 7.3 Season splits (once per year)

When a new season begins, edit `config.py`:

```python
TRAIN_SEASONS = [2024, 2025]      # roll training window forward
VALIDATION_SEASON = 2026
TEST_SEASON = 2027
```

This is a one-line edit, once a year.

---

## 9. The Daily Betting Workflow

Once you're comfortable, your daily routine is roughly **15–20 minutes**:

### Morning (5 minutes)

1. Settle yesterday's bets:
   ```sql
   UPDATE mlb.dbo.fact_pitcher_strikeout_betting_ev
   SET actual_strikeouts = (SELECT actual_strikeouts FROM mlb.dbo.fact_pitcher_game_strikeout_predictions g
                            WHERE g.gamePk = ev.gamePk AND g.pitcher_id = ev.pitcher_id)
   FROM mlb.dbo.fact_pitcher_strikeout_betting_ev ev
   WHERE actual_strikeouts IS NULL;
   ```
2. Run `compute_ev.sql` to mark WIN/LOSS on yesterday's bets.
3. Glance at performance (see section 11).

### Afternoon (10 minutes)

1. Pull tonight's slate:
   ```sql
   SELECT pitcher_name, opponent_team_name, predicted_strikeouts,
          predicted_k_stddev, prob_over_5_5, prob_over_6_5
   FROM mlb.dbo.fact_pitcher_game_strikeout_predictions
   WHERE game_date = CAST(GETDATE() AS DATE)
   ORDER BY predicted_strikeouts DESC;
   ```

2. Open your sportsbook. For each interesting pitcher, write down the line and over/under odds.

3. Update the EV table:
   ```sql
   UPDATE mlb.dbo.fact_pitcher_strikeout_betting_ev
   SET sportsbook = 'Sportsbet', line = 5.5, over_odds = 1.90, under_odds = 1.90
   WHERE gamePk = 824932 AND pitcher_id = 691725;
   ```

4. Run `compute_ev.sql`.

5. Read the recommendations:
   ```sql
   SELECT pitcher_name, opponent_team_name, line, predicted_strikeouts,
          model_prob_over, over_odds, edge_over, ev_over,
          recommended_side, kelly_fraction
   FROM mlb.dbo.fact_pitcher_strikeout_betting_ev
   WHERE recommended_side IN ('OVER', 'UNDER')
     AND game_date = CAST(GETDATE() AS DATE)
   ORDER BY ev_over DESC;
   ```

### Pre-game (5 minutes)

1. Place each recommended bet at the suggested Kelly fraction.
2. **Cap any single bet at 5% of bankroll** even if Kelly says higher.
3. Check confirmed lineups before final placement — late scratches invalidate predictions.

---

## 10. Metrics Reference: What Good and Bad Look Like

These are the numbers that tell you whether the model is working well. Find them in `fact_model_evaluation_metrics`.

### Per-PA classifier metrics

#### **ROC AUC** (`roc_auc`)

What it measures: How well the model ranks PAs likely to end in K above PAs unlikely to end in K. 0.5 = random guessing, 1.0 = perfect.

**Important caveat for this model:** because the target is fractional (per-PA K rate, not binary 0/1), AUC is computed by binarising the target at 0.5 — i.e., asking "did this hitter strike out in more than half of their PAs?" That's a rare event for most hitters, so the AUC numbers here run lower than for a typical binary classifier. **Don't compare these AUCs to numbers from other ML projects.**

| Value | Verdict |
|---|---|
| < 0.55 | **Bad** — model isn't learning |
| 0.55 – 0.58 | **Acceptable** — typical for fractional K-rate target |
| 0.58 – 0.62 | **Good** — what we expect from a well-tuned model |
| 0.62 – 0.66 | **Excellent** — strong signal |
| > 0.70 | **Suspicious** — likely data leakage, investigate |

**For betting, AUC matters less than Brier.** Two models with identical Brier but different AUC will perform similarly in your bankroll. AUC measures *ranking*; Brier measures *calibration*. Calibration is what turns into dollars.

#### **Brier Score** (`brier`)

What it measures: How calibrated the probabilities are. Lower is better. **This is the most important metric for betting.** A model with great AUC but bad Brier will lose you money.

| Value | Verdict |
|---|---|
| > 0.20 | **Bad** — uncalibrated probabilities |
| 0.18 – 0.20 | **Poor** |
| 0.16 – 0.18 | **Acceptable** |
| 0.14 – 0.16 | **Good** |
| 0.12 – 0.14 | **Excellent** |
| < 0.12 | **Suspicious** for skewed datasets |

**Note:** Brier is computed against the fractional target directly, so `(prob - rate)²` rather than `(prob - 0_or_1)²`. This is more honest than the binary version and is the metric you should trust most.

#### **Log Loss** (`log_loss_val`)

What it measures: Penalty for confidently wrong predictions. Lower is better.

| Value | Verdict |
|---|---|
| > 0.55 | **Bad** |
| 0.50 – 0.55 | **Acceptable** |
| 0.45 – 0.50 | **Good** |
| 0.40 – 0.45 | **Excellent** |
| < 0.40 | **Suspicious** |

#### **Accuracy** (`accuracy`)

What it measures: Percentage of PAs the model correctly classifies as K or not. Looks impressive but is misleading because most PAs aren't strikeouts.

For betting purposes, **ignore accuracy** and focus on Brier and AUC. Predicting "no strikeout" for every PA gets 77% accuracy on its own — that's not skill.

### Game-level metrics

#### **MAE** (Mean Absolute Error)

What it measures: Average miss in predicted total Ks per pitcher-game.

| Value | Verdict |
|---|---|
| > 2.0 | **Bad** |
| 1.7 – 2.0 | **Poor** |
| 1.4 – 1.7 | **Acceptable** — typical for K prediction |
| 1.2 – 1.4 | **Good** |
| 1.0 – 1.2 | **Excellent** |
| < 1.0 | **Suspicious** unless many short outings |

#### **Bias**

What it measures: Whether the model systematically over- or under-predicts. Should be near zero.

| Value | Verdict |
|---|---|
| -0.5 to -0.2 | **Under-predicting** — model is systematically too low |
| -0.2 to +0.2 | **Good** — close to unbiased |
| +0.2 to +0.5 | **Over-predicting** — model is systematically too high (current state — patched in EV script) |
| beyond ±0.5 | **Bad** — calibration is broken |

**Note on the current model:** the bias on the test set is around +0.3, which is just outside the "Good" range. This is patched in `compute_ev.sql` via per-bucket bias correction (see section 11) so it doesn't affect bet recommendations, but the underlying number is what you'd see in `fact_model_evaluation_metrics`.

### How to read your results dashboard

Every time you run the pipeline, run this query:

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

This is where the rubber meets the road. The EV script gives you recommendations, but you should understand **why** a bet is being recommended.

### The metrics you care about per-bet

Every row in `fact_pitcher_strikeout_betting_ev` (after running the EV script) has these:

#### **edge_over** / **edge_under**

What it measures: How much higher your model's probability is than the sportsbook's implied probability.

| Value | Verdict |
|---|---|
| < 0.03 | **Pass** — too thin, eaten by line movement and vig |
| 0.03 – 0.06 | **Marginal** — below the script's threshold (6%) |
| 0.06 – 0.10 | **Good** edge — the script's recommended threshold |
| 0.10 – 0.15 | **Strong** edge — bet with confidence |
| 0.15 – 0.25 | **Excellent** edge — rare and worth a bigger stake |
| > 0.25 | **Suspicious** — sportsbook usually doesn't make 25%+ mistakes; double-check the line and lineup |

#### **ev_over** / **ev_under**

What it measures: Expected return on a $1 bet. **The script only flags bets with EV > 7%** (raised from 5% to absorb residual model uncertainty after the bias-correction patches).

| Value | Verdict |
|---|---|
| < 0 | **Negative EV** — never bet (the script won't recommend it) |
| 0 – 0.07 | **Below threshold** — skip (script will mark as PASS) |
| 0.07 – 0.12 | **Acceptable** EV — script's minimum |
| 0.12 – 0.20 | **Good** EV |
| 0.20 – 0.30 | **Strong** EV |
| > 0.30 | **Excellent** EV (verify it's not too good to be true) |

#### **kelly_fraction**

What it measures: Quarter-Kelly stake suggestion as a fraction of bankroll. **The script hard-caps this at 0.05** (5% of bankroll), so you'll never see values above that — even when raw Kelly says you should stake more.

| Value | Suggested action |
|---|---|
| 0 | Don't bet (script flagged PASS) |
| 0.01 – 0.02 | Small stake (1–2% of bankroll) |
| 0.02 – 0.04 | Moderate stake (2–4%) |
| 0.04 – 0.05 | Large stake (4–5%) — script's hard cap |

**The 5% cap is non-negotiable.** Model uncertainty is real, and a single 50% raw Kelly bet that loses can wipe out months of grinding. The cap is in the SQL script itself; you don't need to remember it.

#### **predicted_k_stddev**

What it measures: Uncertainty in the K total prediction.

| Value | Verdict |
|---|---|
| < 1.5 | **High confidence** — bet normally |
| 1.5 – 2.0 | **Moderate confidence** — bet normally |
| 2.0 – 2.5 | **Lower confidence** — consider reducing stake |
| > 2.5 | **High variance** — the model is uncertain. Halve the Kelly stake or skip. |

### Putting it together: a "good bet" checklist

The EV script does most of the gating for you. A bet is genuinely worth placing when **all** of these are true:

- [ ] `recommended_side` is OVER or UNDER (not PASS)
- [ ] `edge_over` (or `edge_under`) ≥ 0.06 (script's threshold)
- [ ] `ev_over` (or `ev_under`) ≥ 0.07 (script's threshold)
- [ ] `predicted_strikeouts` < 8.0 (script auto-PASSes anything ≥ 8 — see "bias correction" below)
- [ ] `predicted_k_stddev` < 2.2
- [ ] Lineup is confirmed (no late scratches)
- [ ] You haven't already exceeded daily bankroll budget (suggested: cap at 15% of bankroll across all bets in one day)

If any are false, skip it. There will be more games tomorrow.

### About the bias correction in the EV script

The model has a known issue: it over-predicts more for high-K pitchers than for low-K pitchers. We measured this from the test set:

| Predicted Ks | Observed bias | Correction applied |
|---|---|---|
| < 4 | +0.004 (essentially perfect) | None |
| 4 – 6 | +0.22 | Subtract 0.20 from prediction |
| 6 – 8 | +0.63 | Subtract 0.60 from prediction |
| ≥ 8 | +2.00 (n=8 — too small to trust) | **Auto-PASS — never bet** |

**The `compute_ev.sql` script applies these corrections automatically.** You don't need to do anything manually. When you run the script, the `model_prob_over` column reflects the corrected prediction, not the raw one.

The reporting query at the end of the script shows you both the raw and bias-corrected predictions side-by-side so you can see what the correction did:

```
raw_predicted_K  bias_corrected_K  model_prob_over  recommended_side
8.4              7.8               0.71              OVER
6.6              6.0               0.58              OVER
8.5              8.5 (no correct.) 0.62              PASS  ← bucket 4, auto-PASS
```

**Rebuild this correction every 3-4 months once you've accumulated 100+ bets** — see section 15 (Maintenance).

### Example interpretations

**Example 1 — clear bet:**
```
pitcher_name: Spencer Strider
line: 6.5
raw_predicted_K: 8.4
bias_corrected_K: 7.8        (subtracted 0.6 - bucket 3)
predicted_k_stddev: 1.8
model_prob_over: 0.69         (computed from corrected mean)
over_odds: 1.95
edge_over: 0.182
ev_over: 0.346
recommended_side: OVER
kelly_fraction: 0.0500        (capped at 5%)
```
Edge is 18.2%, EV is 34.6%, stddev is comfortable, bias-corrected prediction still well above the line. Bet OVER at 5% of bankroll (Kelly cap).

**Example 2 — pass it:**
```
pitcher_name: Logan Webb
line: 5.5
raw_predicted_K: 5.7
bias_corrected_K: 5.5         (subtracted 0.2 - bucket 2)
predicted_k_stddev: 2.4
model_prob_over: 0.50         (sits right on the line after correction)
over_odds: 1.92
edge_over: 0.020
ev_over: 0.040
recommended_side: PASS
```
Edge is below 6% threshold, EV is below 7% threshold, the script flagged PASS for a reason. **Don't override the script's PASS.**

**Example 3 — auto-PASS (high-K bucket):**
```
pitcher_name: Elite Pitcher
line: 7.5
raw_predicted_K: 8.3
bias_corrected_K: 8.3         (no correction - bucket 4 is auto-PASSed)
predicted_k_stddev: 2.1
model_prob_over: 0.61         (from raw prediction)
over_odds: 1.85
edge_over: 0.070
ev_over: 0.128
recommended_side: PASS        (auto-PASSed because predicted ≥ 8.0)
```
Even though edge and EV both look acceptable on paper, the script auto-PASSes because predictions ≥ 8 K are unreliable (only 8 games of data, bias was +2.0). After accumulating 30+ such games over time, you can revisit this rule.

**Example 4 — too good to be true:**
```
pitcher_name: Random Reliever
line: 3.5
predicted_strikeouts: 7.2
predicted_k_stddev: 3.1
model_prob_over: 0.85
over_odds: 2.40
edge_over: 0.433
ev_over: 1.04
recommended_side: OVER
```
A 43% edge is almost never real. Investigate before betting:
- Is this actually a reliever (not a starter) with the wrong line?
- Has the lineup changed since the prediction?
- Is the source data correct for this game?

If you can't find a reason for the discrepancy, skip it. The book is rarely that wrong.

---

## 12. Handedness — Reading Platoon Matchups

The model now uses **batter handedness and pitcher throwing arm** as direct features. This section explains what's exposed in the output tables and how to read it for bet decisions.

### Why handedness matters

Same-side matchups (RHP vs RHB, LHP vs LHB) produce K rates 2-4 percentage points higher than opposite-side matchups. Some pitchers have huge platoon splits (devastating vs same-handed batters, average vs opposite); others are flat. For a starter facing a lineup, the lineup's handedness mix can swing the predicted K total by 0.5-1.5 Ks compared to a "platoon-neutral" lineup.

### What handedness columns are now in your tables

#### `fact_pa_strikeout_predictions` — per-PA detail

Each row now shows:

| Column | Values | Meaning |
|---|---|---|
| `hitter_bats` | 'L', 'R', 'S' | Hitter's batting side. 'S' = switch hitter |
| `pitcher_throws` | 'L', 'R' | Pitcher's throwing arm |
| `platoon_matchup` | 'Same', 'Opposite', 'Switch' | Convenience label |

#### `fact_pitcher_game_strikeout_predictions` — daily betting board

Each row now shows the full lineup composition the pitcher faced:

| Column | Meaning |
|---|---|
| `pitcher_throws` | 'L' or 'R' |
| `opp_lhb_count` | Total left-handed batter PAs in the lineup |
| `opp_rhb_count` | Total right-handed batter PAs |
| `opp_switch_count` | Total switch-hitter PAs |
| `opp_same_side_count` | PAs where pitcher and batter are same-handed (best for K) |
| `opp_opposite_side_count` | PAs where pitcher and batter are opposite-handed |

These are PA counts, not hitter counts — i.e. a hitter who faces the pitcher 3 times contributes 3 to the relevant count.

#### `fact_pitcher_strikeout_betting_ev` — your EV worksheet

Same lineup composition columns as the game predictions table, plus `pitcher_throws`. You don't need to enter these — they're filled by the pipeline.

### How to read these for bet decisions

#### The "platoon advantage" diagnostic

Compute the same-side ratio for each pitcher-game:

```sql
SELECT
    pitcher_name,
    pitcher_throws,
    opponent_team_name,
    predicted_strikeouts,
    opp_lhb_count,
    opp_rhb_count,
    opp_switch_count,
    opp_same_side_count,
    opp_opposite_side_count,
    -- Higher = more platoon advantage = K-friendly matchup
    CAST(opp_same_side_count AS FLOAT) /
        NULLIF(opp_same_side_count + opp_opposite_side_count, 0)
        AS same_side_ratio
FROM mlb.dbo.fact_pitcher_game_strikeout_predictions
WHERE game_date = CAST(GETDATE() AS DATE)
ORDER BY same_side_ratio DESC;
```

**What `same_side_ratio` means:**

| Value | Interpretation |
|---|---|
| > 0.55 | **Strong platoon edge** — pitcher faces mostly same-handed batters |
| 0.45 – 0.55 | **Mixed** — typical lineup |
| < 0.45 | **Reverse platoon** — pitcher faces mostly opposite-handed batters (harder for K) |

**Practical implication:** The model has already factored handedness into the prediction. But if you see a starter facing a heavily mismatched lineup (say `same_side_ratio = 0.70`), that's a context you might lean OVER on if everything else is borderline. Conversely, a `0.30` lineup is a context that supports UNDER.

#### Watch out for switch-hitter heavy lineups

If `opp_switch_count` is high (say > 6 PAs), the pitcher faces many hitters who effectively have platoon advantage every PA. This is K-suppressing. Some lineups (Yankees, Twins) often run 3+ switch hitters. If the model's prediction looks high on that day, the platoon context isn't supporting it.

#### Sanity check for left-handed pitchers

LHPs typically face RHB-heavy lineups because most MLB hitters are right-handed. If you see a LHP with `opp_same_side_count > 9`, that's an unusually favorable matchup — the model already knows this but it's a flag worth noticing.

### Example — full read of a pitcher-game

```
pitcher_name: Parker Messick
pitcher_throws: L
opponent_team_name: Atlanta Braves
predicted_strikeouts: 7.24
opp_lhb_count: 6
opp_rhb_count: 18
opp_switch_count: 3
opp_same_side_count: 6        ← only 6 PAs with platoon advantage
opp_opposite_side_count: 21   ← 21 PAs where the Braves' hitters have advantage
same_side_ratio: 0.22         ← reverse-platoon lineup
```

This is a tough K matchup for Messick — most batters he faces have the platoon advantage. The model prediction of 7.24 already accounts for this; if it had been a same-side-heavy lineup, the prediction would have been higher. But knowing this helps you trust an UNDER lean if odds come in close.

### What the model learned about platoon

Once the pipeline has been re-trained with handedness features included, you can verify the model is using them. Run this:

```sql
-- Compare avg predicted K probability by platoon matchup
SELECT
    platoon_matchup,
    COUNT(*) AS n,
    AVG(prob_strikeout) AS avg_pred_k_prob
FROM mlb.dbo.fact_pa_strikeout_predictions
WHERE split_set IN ('validation', 'test')
GROUP BY platoon_matchup
ORDER BY avg_pred_k_prob DESC;
```

**Healthy result:** `Same` should average ~2-4 percentage points higher than `Opposite`. If they're equal, the model isn't using handedness effectively.

---

## 13. Things to Watch Out For

### Calibration drift

If `Brier` starts climbing across pipeline runs (e.g. 0.137 → 0.145 → 0.152), the model is decalibrating. Re-train on fresh data. This usually happens after major league-wide changes (new ball, new pitch clock rule, etc.).

### Bias drift

If overall `bias` consistently exceeds ±0.4 across runs, the model is drifting from where the bias correction was calibrated. Re-run the bucket diagnostic in section 15 and update the correction values in `compute_ev.sql`. If retraining doesn't fix it, the lineup encoding or a feature has changed upstream.

### Suspicious AUC jumps

If a new run suddenly produces AUC > 0.70 when previous runs were ~0.58, **don't celebrate — investigate**. Almost always this means a leakage column got into the features. Check `LEAKAGE_COLS` in `config.py`.

### Late lineup changes

The model uses the most recent lineup it saw. If a star sits at game time and the predictions weren't refreshed, the prediction is wrong. **Always check confirmed lineups before placing significant bets.**

### Sportsbook line movement

Lines move. The 1.90 you saw at lunch might be 1.75 at game time after sharp money hits. Either bet early or be willing to skip if the line moves against you.

### Over-confidence after a win streak

You will go on hot streaks. Don't increase your stakes during them — variance is real and a 5-bet win streak doesn't mean your model just got better.

### Tilt after a loss streak

You will also go on cold streaks. A 50-bet sample is not enough to judge whether your edge is real. Stick to the system; don't chase losses.

### The 5% bankroll cap

I keep saying this because it's the single most important rule. **Never stake more than 5% of bankroll on one bet**, no matter how good Kelly says it is. Model uncertainty is real, and a single 50% Kelly bet that loses can wipe out months of grinding.

### Keeping a separate bankroll

Don't bet from your everyday checking account. Set up a dedicated bankroll. Withdraw profits monthly. Treat losses as cost of business — never top up the bankroll from outside funds.

---

## 14. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'config'` | Running as a module from outside | `cd` into the `model_v4` folder first |
| `Login failed for user` | SQL Server auth | Check `sql_loader.py` connection settings |
| `Conversion failed when converting varchar to int` | Column type mismatch in DDL | Drop the table and re-run pipeline.py |
| `recommended_side` is all PASS | No edges today, OR you forgot to enter odds | Check `over_odds` is filled in |
| Duplicate rows in prediction tables | Pipeline ran twice without cleanup | The pipeline now auto-clears before each load. For old duplicates, run `sql/cleanup_duplicates.sql` once |
| AUC dropped below 0.55 | Source data issue, or features changed | Check the `LEAKAGE_COLS` and recent source view changes |
| AUC above 0.70 | Likely leakage | Investigate `LEAKAGE_COLS`; a post-game column probably leaked in |
| Memory error during training | Dataset grew | Reduce `XGB_NUM_ROUNDS` and `LGBM_NUM_ROUNDS` to 500 in config.py |
| `score_future_games.py` says "no future matchups found" | `mlb_schedule` empty for upcoming dates, OR no probable pitchers fetched | Run upstream schedule pipeline + `python fetch_probable_pitchers.py` |
| Future predictions show 0 unique pitchers | MLB API hasn't published probable pitchers yet | Wait. Probable pitchers usually appear 1-3 days before games |
| `Invalid object name 'dbo.fact_pitcher_game_strikeout_predictions'` during scoring | Table was dropped without re-running pipeline | Re-run `python pipeline.py` to recreate the table, OR rely on `score_future_games.py` calling `ensure_output_tables()` automatically |
| Live scoring predictions are all NaN | Hitter or pitcher feature snapshots not being found | Confirm `fact_hitter_pitcher_matchup_with_handedness` has rows for the season — upstream data may be stale |
| `predicted_strikeouts` looks far too low (< 2 for starters) | `hitter_plate_appearances` not populating from source view | Run the diagnostic queries in section 10 — likely a source data issue |
| `predicted_strikeouts` looks too low (< 3) | Reliever included, or short outing in source data | Filter on `batters_faced_modeled >= 15` for true starters |
| `batters_faced_modeled` always 1-3 even for starters | Source view counts hitters as PAs incorrectly | Check that source view's `hitter_plate_appearances` column is populated |
| Average actual_strikeouts is < 4 across all games | Source data only contains partial games | Investigate source view — should report ~4-7 average for starters |

---

## 15. Maintenance

### Daily
- Run upstream data pipelines (Phase A in section 6)
- Run `python run_daily.py` for live scoring of upcoming games
- Settle yesterday's bets via `compute_ev.sql`

### Weekly
- Re-run `pipeline.py` to retrain on the latest data (auto-deduplicates predictions; preserves any rows where you've entered odds)
- Check the metrics dashboard (section 10) for drift

### Monthly
- Review your bet log:
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
- If profit_per_unit_staked is negative after 100+ bets, something's wrong. Re-train and re-evaluate.

### Every 3-4 months (or after 100+ bets)

**Re-tune the bias correction in `compute_ev.sql`.** Run this diagnostic to see if the bucket biases have shifted:

```sql
SELECT
    CASE
        WHEN predicted_strikeouts < 4 THEN '1. Low (<4)'
        WHEN predicted_strikeouts < 6 THEN '2. Mid (4-6)'
        WHEN predicted_strikeouts < 8 THEN '3. High (6-8)'
        ELSE '4. Very High (8+)'
    END AS bucket,
    COUNT(*) AS n_games,
    AVG(predicted_strikeouts) AS avg_pred,
    AVG(CAST(actual_strikeouts AS FLOAT)) AS avg_actual,
    AVG(predicted_strikeouts - CAST(actual_strikeouts AS FLOAT)) AS bias,
    AVG(ABS(predicted_strikeouts - CAST(actual_strikeouts AS FLOAT))) AS mae
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

If the biases have shifted by more than 0.15 in any bucket, update the corresponding numbers in `compute_ev.sql`:

```sql
WHEN predicted_strikeouts < 4.0 THEN <new bias for bucket 1>
WHEN predicted_strikeouts < 6.0 THEN <new bias for bucket 2>
WHEN predicted_strikeouts < 8.0 THEN <new bias for bucket 3>
```

These appear in multiple places in the script — find-and-replace all of them at once.

### Per-season
- Roll the season splits in `config.py`
- Drop the four output tables and re-run for a clean slate
- Compare metrics year-over-year to track real model improvement

---

## Final reminders

1. **You will lose 40–45% of individual bets even with positive EV.** Variance is brutal. Trust the system.
2. **Quarter-Kelly is conservative deliberately.** The script's 5% hard cap is in addition to that — never override either upward.
3. **Cap any single bet at 5% of bankroll.** No exceptions. (The script enforces this automatically.)
4. **Sports betting is gambling.** The model gives you an edge, not a guarantee. Don't bet money you can't afford to lose.
5. **Check confirmed lineups before placing bets.** A late scratch invalidates the prediction.
6. **Track everything.** Your bet log is the only ground truth. Believe the data, not your gut.
7. **Don't bet on predictions ≥ 8 K.** The script auto-PASSes these. After accumulating 30+ such games of data, this rule can be revisited.

Good luck.
