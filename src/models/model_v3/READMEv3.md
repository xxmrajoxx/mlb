# MLB Pitcher Strikeout Model

Predict, for any starter–lineup matchup:

1. **Per-PA**: probability that this plate appearance ends in a strikeout.
2. **Per-game**: expected total strikeouts the starter records, with an 80% interval.

The whole pipeline lives in `mlb_strikeout_model.py`.

---

## 1. Is the source table usable?

**Yes — with the cleanup the script applies.** The source table
`mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2` has the right grain
(one row per hitter–pitcher event in a game) and a strong feature set:
recent-form rolling windows for both hitter and pitcher, head-to-head career
stats, lineup position, pitch-mix and Statcast-derived plate-discipline
features. Across 2023–2026 you have enough volume to train a serious model.

But three things need addressing before training:

| Issue | Fix in script |
|---|---|
| Some rows have `hitter_plate_appearances > 1` and pre-aggregated outcomes (e.g. PA=8 with K=2). The features on those rows are averaged, so they break the per-PA assumption. | `keep_only_single_pa_rows=True` filters to PA=1 rows. |
| Heavy NULLs in early-season 2026 rows because the rolling-window features need history. | LightGBM handles NaN natively; no imputation. The model learns to use the columns that are populated. |
| Several columns are the target itself or derived from it on the same row. | `LEAK_COLS` removes them: `hitter_strikeouts`, `hitter_walks`, `hitter_hits`, `hitter_home_runs`, `y_k_rate`, `sample_weight`. |

---

## 2. Which model — and why

**Primary: gradient-boosted trees (LightGBM).**

Why this beats the alternatives for your problem:

- **Native NaN handling.** Half your rolling-window features are NULL early in
  a season. LightGBM splits on missingness directly. XGBoost does too;
  scikit-learn's tree models do not without imputation that loses signal.
- **Heterogeneous feature scales.** You have rates (0–1), velocities (88–98),
  pitch counts (0–110), career H2H counts (0 to ~50). Trees don't care about
  scaling; logistic regression and neural nets need careful preprocessing for
  no real lift on tabular data this size.
- **You don't have enough data for deep tabular models.** TabNet, FT-Transformer,
  etc. need millions of rows to beat GBMs and they typically don't on tabular
  baseball data. GBMs are also 50× faster to iterate on.
- **Interpretability matters here.** `feature_importance.csv` lets you see
  what's driving predictions, and SHAP works cleanly on top of LightGBM if
  you want per-prediction explanations later.

**Calibration: isotonic regression on the validation fold.** This step is not
optional. We sum per-PA probabilities to get game-level expected K's, so the
probabilities have to mean what they say. A LightGBM raw output can be
miscalibrated (especially near the tails); isotonic fixes that.

**Baseline: regularized logistic regression.** It's run alongside as a sanity
check. If LightGBM's logloss is barely better than LR's, the feature set is
the bottleneck, not the model.

**On Poisson regression.** You'll see Poisson recommended for "count"
problems. It's the wrong tool here. Each row is one Bernoulli trial (1 PA,
either K or not-K), and the game total is a *sum of Bernoullis*, which is
Poisson-binomial — not Poisson. Modelling per-PA K probability and summing
gives a tighter, better-calibrated answer than fitting a Poisson on game-level
strikeout counts directly.

---

## 3. Features removed and why

The source has ~250 columns. The script keeps ~140 after these cuts.

**Hard removals (leakage or non-features):**

```
hitter_strikeouts        — the target itself
hitter_walks             — same-PA outcome
hitter_hits              — same-PA outcome
hitter_home_runs         — same-PA outcome
y_k_rate                 — = strikeouts / PA on the same row
sample_weight            — equals PA; encodes the outcome's row-shape
```

**ID columns (kept for joining the output table, dropped from the feature
matrix):**

```
gamePk, game_date, season,
hitter_id, hitter_name, hitter_position, hitter_team_id, hitter_team_name,
pitcher_id, pitcher_name, pitcher_team_id, pitcher_team_name,
hitter_plate_appearances    (used as a denominator only)
```

**Redundant rolling windows:**

For every stat, the table has `_avg_last_3 / _avg_last_5 / _avg_last_10` AND
`_weighted_last_3 / _weighted_last_5 / _weighted_last_10`. The weighted versions
already encode recency-decay weighting, so the plain `_avg_last_5` and
`_avg_last_10` are pure duplicates. We keep:

- All `_weighted_*_last_5` and `_weighted_*_last_10` columns
- Plain `_avg_*_last_3` (most-recent-form snapshot is genuinely different signal)
- Drop plain `_avg_*_last_5` and `_avg_*_last_10`

This cuts ~40 redundant features without losing anything.

**Auto-dropped strings:** Any object-dtype column the script encounters is
dropped from the feature matrix automatically. `hitter_lineup_position_name`
is the obvious one — it's a redundant string twin of the integer
`hitter_lineup_position`.

The exact list of features used in the trained model is written to
`outputs/features_used.json`.

---

## 4. The output tables

The script writes two CSVs:

### `predictions_per_PA.csv` — one row per (game × pitcher × hitter × PA slot)

Use this when you want hitter-by-hitter detail for a specific matchup.

| Column | What it is |
|---|---|
| `gamePk`, `game_date`, `season` | Game identifiers |
| `pitcher_id`, `pitcher_name`, `pitcher_team_name` | Pitcher info |
| `hitter_id`, `hitter_name`, `hitter_team_name` | Hitter info |
| `hitter_lineup_position`, `hitter_batting_order` | Where the hitter slots in |
| `first_inning_faced` | Inning the matchup begins |
| `pitcher_is_starter`, `pitcher_gamesStarted`, `pitcher_days_since_last_appearance` | Role/rest |
| `pitcher_prev_*` | Pitcher's previous-game line (K, IP, BF, pitches, K/9) |
| `pitcher_avg_*_last_3`, `pitcher_weighted_*_last_5/10` | Rolling form (recent K/BF, K/9, strike%, whiff%, CSW%, putaway%, chase%, velocity, zone%) |
| `hitter_prev_*` | Hitter's previous game (K, PA, AB, H, HR, BB, OPS, K-rate) |
| `hitter_avg_k_last_3`, `hitter_weighted_*_last_5` | Hitter rolling form (K-rate, walk-rate, AVG, OPS, whiff%, contact%, chase%, two-strike whiff%, CSW% against) |
| `hitter_avg_exit_velocity_last_5`, `hitter_avg_xwoba_last_5` | Quality of contact |
| `h2h_career_*`, `is_first_matchup` | Head-to-head history |
| `hitter_plate_appearances` | PAs in this row (will be 1 for training rows) |
| `hitter_strikeouts` | Actual K's in this row (for backtesting only) |
| **`k_prob_calibrated`** | **The headline number — calibrated P(K) for this PA.** |
| `k_prob_raw` | Uncalibrated LightGBM output (for diagnostics) |
| `expected_k` | Same as calibrated probability (this PA's contribution to E[K]) |
| `k_variance` | `p (1-p)`; used when summing to lineup level |

### `predictions_pitcher_vs_lineup.csv` — one row per (game × pitcher)

Use this for the "how many K's tonight?" view.

| Column | What it is |
|---|---|
| `gamePk`, `game_date`, `season` | Game identifiers |
| `pitcher_id`, `pitcher_name`, `pitcher_team_name` | Pitcher info |
| `hitter_team_name` | Opposing lineup |
| `total_PA_faced` | PAs the pitcher actually had vs this lineup |
| `total_K_observed` | Actual K's recorded (for backtest only) |
| **`expected_K`** | **Sum of calibrated PA probabilities — the model's E[K]** |
| `variance_K` | `Σ p(1-p)` across PAs |
| `std_K` | `sqrt(variance_K)` |
| `k_lower_80`, `k_upper_80` | 80% interval (normal approximation; tight for n ≥ 10 PAs) |
| `k_above_expected` | `total_K_observed - expected_K` (model performance per game) |
| `n_unique_hitters` | Distinct hitters faced |
| `avg_PA_K_prob` | Mean per-PA K probability across the lineup |
| `max_PA_K_prob`, `min_PA_K_prob` | Hardest / softest matchup in the lineup |

The math behind `expected_K`: every PA is approximately a Bernoulli trial
with probability `p_i`. The total K count is a Poisson-binomial sum:

```
E[K]   = Σ p_i
Var[K] = Σ p_i (1 - p_i)
```

A starter faces 18–28 PAs, so the normal approximation for the interval is
fine. If you ever want it exact, use the Poisson-binomial PMF.

---

## 5. How to run it

### Install

```bash
pip install lightgbm scikit-learn pandas numpy sqlalchemy pyodbc
```

You also need the Microsoft ODBC Driver for SQL Server on the machine running
this. On Linux: install `msodbcsql18`. On Windows: it's usually already there.

### Export the source table

Pull `mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2` to a TSV with
`NULL` as the null token (matches the format the script expects):

```sql
-- example: SQL Server bcp / SSMS export
SELECT * FROM mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2
```

If you export as CSV, change `sep="\t"` to `sep=","` in `load_data()`.

### Run (with SQL Server load)

```bash
python mlb_strikeout_model.py \
  --input fact_hitter_pitcher_matchup_model_featuresv2.tsv \
  --out outputs \
  --sql-server "your-sqlhost.example.com,1433" \
  --sql-database mlb \
  --sql-schema dbo \
  --sql-user "your_user" \
  --sql-password "your_password"
```

For Windows integrated auth, replace the user/password flags with `--sql-trusted`.

You can also set credentials via environment variables instead of CLI flags:

```bash
export MSSQL_SERVER="your-sqlhost.example.com,1433"
export MSSQL_DATABASE=mlb
export MSSQL_USER=your_user
export MSSQL_PASSWORD=your_password
# or: export MSSQL_TRUSTED=1

python mlb_strikeout_model.py --input data.tsv --out outputs
```

### Run (CSVs only, skip SQL)

```bash
python mlb_strikeout_model.py --input data.tsv --out outputs --no-sql
```

### What gets written

To disk (`outputs/`):
```
lightgbm_model.txt                       # serialized trained model
features_used.json                       # exact feature list
feature_importance.csv                   # ranked by gain
predictions_per_PA.csv                   # hitter-by-hitter
predictions_pitcher_vs_lineup.csv        # game-level
```

To SQL Server (`mlb.dbo`):
```
fact_pitcher_strikeout_predictions_per_pa
fact_pitcher_strikeout_predictions_game
```

---

## 5b. SQL Server load — what it actually does

### Tables created

| Table | Grain | Use for |
|---|---|---|
| `mlb.dbo.fact_pitcher_strikeout_predictions_per_pa` | one row per (game × pitcher × hitter) | hitter-by-hitter matchup detail |
| `mlb.dbo.fact_pitcher_strikeout_predictions_game` | one row per (game × pitcher) | "how many K's tonight?" view |

Both tables include:

- **`prediction_generated_at`** — UTC timestamp set once per pipeline run, on
  every row. Lets you tell two backfills apart and audit when a prediction
  was made.
- **Sort + clustered index** by `pitcher_team_name` ASC, then
  `pitcher_is_starter` DESC (starters first), then `game_date` ASC,
  `pitcher_name` ASC. The script issues `CREATE CLUSTERED INDEX` after the
  bulk load so the rows are physically stored in that order on disk —
  range scans by team or starter benefit immediately.

### Load mode

By default, `--sql-if-exists replace` drops and recreates each table per run.
Safe and idempotent. Use `--sql-if-exists append` to keep history across
runs (in which case `prediction_generated_at` is what tells one run from
another).

### Connection details

Connection string is built from these (CLI flag → env var):

| CLI | Env var | Default |
|---|---|---|
| `--sql-server` | `MSSQL_SERVER` | (required) |
| `--sql-database` | `MSSQL_DATABASE` | `mlb` |
| `--sql-schema` | — | `dbo` |
| `--sql-user` | `MSSQL_USER` | (required if not trusted) |
| `--sql-password` | `MSSQL_PASSWORD` | (required if not trusted) |
| `--sql-driver` | `MSSQL_DRIVER` | `ODBC Driver 18 for SQL Server` |
| `--sql-trusted` | `MSSQL_TRUSTED` | off |
| `--sql-table-per-pa` | — | `fact_pitcher_strikeout_predictions_per_pa` |
| `--sql-table-game` | — | `fact_pitcher_strikeout_predictions_game` |
| `--sql-if-exists` | — | `replace` |

The engine uses `fast_executemany=True` for fast bulk inserts. Loading 200k
rows takes a few seconds on a normal connection.

### Column types in SQL Server

The script passes an explicit `dtype` mapping to `to_sql` rather than letting
pandas guess. Without this, every string column ends up as `NVARCHAR(MAX)`
and every integer as `BIGINT`, which is wasteful. Highlights:

- IDs (`gamePk`, `pitcher_id`, `hitter_id`) → `BIGINT`
- Small integers (lineup position, inning, PA, K) → `SMALLINT`
- Names and team names → `NVARCHAR(100)`
- Probabilities and stats → `FLOAT`
- `game_date`, `prediction_generated_at` → `DATETIME2`

### Sample queries

Tonight's starters and their projected K's, ordered the way the table is sorted:

```sql
SELECT
    pitcher_team_name,
    pitcher_name,
    hitter_team_name,
    total_PA_faced,
    expected_K,
    k_lower_80,
    k_upper_80,
    avg_PA_K_prob
FROM mlb.dbo.fact_pitcher_strikeout_predictions_game
WHERE game_date = CAST(GETDATE() AS DATE)
  AND pitcher_is_starter = 1
ORDER BY expected_K DESC;
```

Hitter-by-hitter breakdown for a specific starter:

```sql
SELECT
    hitter_lineup_position,
    hitter_name,
    k_prob_calibrated,
    h2h_career_pa, h2h_career_k,
    hitter_weighted_k_rate_last_5,
    hitter_weighted_two_strike_whiff_rate_last_5
FROM mlb.dbo.fact_pitcher_strikeout_predictions_per_pa
WHERE pitcher_name = 'Aaron Ashby'
  AND game_date = '2026-04-16'
ORDER BY hitter_lineup_position;
```

---

## 6. How the data is split

Time-based, no random shuffling:

```
train: seasons 2023, 2024
val:   season  2025          (used for early stopping AND calibration)
test:  season  2026          (held out, for honest eval only)
```

Random splits leak in baseball — the same pitcher's hot streak shows up in
both train and val, and the model picks up patterns that won't generalize.
Time splits force the model to predict a future it hasn't seen, which is the
real-world setting.

If your export has data only from 2025 or 2026, the script falls back to a
chronological 70/15/15 split within whatever's there.

---

## 7. Metrics to watch

For **PA-level binary classification**:

- **Log loss** — the loss function; lower is better. Most direct measure of
  probability quality.
- **Brier score** — squared error of probability vs outcome; penalises
  overconfidence.
- **AUC** — ranking quality. Realistic ceiling on this problem is ~0.72.
  Anything above 0.65 is genuinely useful.
- **PR-AUC** — better than AUC when classes are imbalanced (K-rate is ~23%).
- **k_rate_pred vs k_rate_true** — global calibration check. After isotonic
  these should be within 0.5 percentage points.

For **game-level expected K's**:

- The headline diagnostic is `k_above_expected` over the test set. It should
  be **mean-zero** and **roughly normal** with no systematic season/team bias.
  If it skews positive or negative across many games, the model is biased.
- RMSE of `expected_K` vs `total_K_observed` is the simplest scalar.

---

## 8. Known limitations and what to extend next

- **No park or weather features** in the source table. Both move K-rate
  measurably (high humidity = fewer K's, dome stadiums = more). Adding even
  ballpark IDs as a categorical would likely help.
- **No platoon split.** Hitter-vs-LHP / RHP whiff splits are powerful and
  partially in the source via `whiff_vs_rhb`/`whiff_vs_lhb`, but pitcher
  handedness itself isn't in the feature list. Add `pitcher_throws` and
  `hitter_bats`.
- **No pitch-count fatigue model.** As a starter passes 80 pitches, K-rate
  drops. The model captures this implicitly via batting order (3rd time
  through), but a `pitches_thrown_in_game_so_far` column would tighten it.
- **First-time-up bias.** The first inning faced is in the data; consider
  adding "times through the order" explicitly.
- **Calibration drift.** Refit isotonic on a recent rolling window in
  production — not just the original validation fold.
- **For per-pitcher props (over/under N strikeouts)**: convert the
  Poisson-binomial distribution into a CDF and read off P(K ≥ N). The
  `expected_K` and `std_K` columns let you do this with a normal approximation
  immediately, or you can compute it exactly from the per-PA probabilities.

---

## 9. File layout

```
.
├── mlb_strikeout_model.py     # full pipeline (load → train → calibrate → predict → output)
└── README.md                  # this file
```

The script is intentionally a single file: easy to read, easy to drop into
any environment, no package layout to fight with. When you're ready to
productionise, the obvious split is `data.py` (load/clean/split),
`model.py` (LightGBM + calibration), `predict.py` (inference + output tables).
