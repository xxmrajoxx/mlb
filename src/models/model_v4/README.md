# MLB Strikeout Prediction & Betting EV System

Predicts how many strikeouts a starting pitcher will rack up against a given lineup, then surfaces over/under bets with positive expected value at Australian sportsbook odds.

---

## What this system does

**Two predictions, layered:**

1. **Per plate-appearance**: for every hitter–pitcher matchup in a game, predict P(strikeout). This is a binary classification problem.
2. **Per pitcher-game**: sum those P(K) values across the lineup the starter will face, producing an expected total Ks for the game *and* a full probability distribution over possible K totals (0, 1, 2, …, n).

The game-level distribution is what powers betting. If you know P(K = 7) precisely, you also know P(K > 5.5), P(K > 6.5), etc., which is what sportsbooks price.

**Three outputs to SQL Server:**

| Table | What it holds | Who fills it |
|---|---|---|
| `fact_pa_strikeout_predictions` | One row per plate appearance with calibrated P(K) | Pipeline |
| `fact_pitcher_game_strikeout_predictions` | One row per pitcher-game with predicted total Ks + over-line probabilities | Pipeline |
| `fact_pitcher_strikeout_betting_ev` | Same key as above, plus columns for sportsbook odds (you fill these in) and computed edge / EV / recommended bet | You + the SQL EV script |
| `fact_model_evaluation_metrics` | Performance metrics (AUC, Brier, log loss, MAE, etc.) per training run | Pipeline |

---

## Why these specific models

You asked for a "complex" model and to use multiple ML algorithms. Here's the actual reasoning:

### Per-PA classifier (the engine)

Three base learners, then calibrated, then ensembled:

- **XGBoost** — by far the strongest single model on tabular MLB data in published research. Handles missing values natively (and there are a lot in the source view — pitcher previous-game features are NULL for first appearance of the season, etc.). Weight: 50% of the ensemble.
- **LightGBM** — same family of model (gradient-boosted trees) but with different splitting heuristics. Including it catches cases where XGB's specific tree structure overfits a quirk. Weight: 40%.
- **Logistic Regression** — a linear baseline. It's not as accurate, but it's there to (a) sanity-check that the trees are doing better than a linear combination of features, and (b) provide diversity in the ensemble — a linear model makes very different errors than a tree. Weight: 10%.

### Why not just XGBoost?

A single model's probabilities are usually overconfident at the extremes (it'll say 95% when the true rate is 88%). Ensembling reduces variance and softens those extremes, both of which improve **calibration** — and calibration is what matters for betting, not raw accuracy.

### Isotonic regression — the most important piece

After each base model trains, we fit an `IsotonicRegression` on its validation-set output to map raw probabilities to *actual* observed frequencies. We do it once per base model, then again on top of the ensemble.

If a model is uncalibrated and outputs 0.30 when the real rate is 0.25, every "over 5.5 Ks at 2.10" bet you make based on that probability will be a slow loser even if the model has high AUC. Calibration fixes this. Brier score in the metrics table is the headline number for calibration quality.

### Why I rejected some other approaches

- **Direct game-level regression** (predict total Ks straight from a pitcher-level feature row): less data per model, no per-batter granularity, can't generate over-line probabilities easily. The PA-then-aggregate path is strictly more expressive.
- **Poisson regression on game totals**: a reasonable alternative, but assumes Ks are Poisson-distributed which they're not — they're Poisson-binomial (each batter has a different P(K)). The aggregation step in `game_aggregation.py` computes the *exact* Poisson-binomial distribution, which is more accurate than a Poisson approximation.
- **Neural nets / TabNet / MLPs**: tested in the literature, generally tie or lose to XGBoost on tabular MLB data unless you have millions of rows and very rich raw inputs (pitch-by-pitch Statcast). Not worth the complexity here.

---

## How the data is split

**No random shuffling.** Time-series data must be split chronologically or you'll leak future information into the training set.

| Split | Seasons | Purpose |
|---|---|---|
| Train | 2023, 2024 | Fit XGB, LGBM, LogReg |
| Validation | 2025 | Early stopping, isotonic calibration, ensemble tuning |
| Test | 2026 | Held out — never touched during training. Real-world performance comes from this split. |

Edit `TRAIN_SEASONS`, `VALIDATION_SEASON`, `TEST_SEASON` in `config.py` if you want to roll the windows forward later in the year.

---

## File layout

```
mlb_strikeout_model/
├── README.md               ← you are here
├── config.py               ← all tuning knobs, table names, splits, hyperparams
├── data_loader.py          ← reads SQL, splits by season
├── features.py             ← column selection, categorical encoding, target prep
├── models.py               ← XGB / LGBM / LogReg / Isotonic / ensemble
├── evaluation.py           ← classification + game-level metrics
├── game_aggregation.py     ← per-PA → pitcher-game (Poisson-binomial PMF)
├── pipeline.py             ← run this
├── sql_loader.py           ← your existing SQL helper (unchanged)
└── sql/
    └── compute_ev.sql      ← run this AFTER you fill in odds
```

---

## Running it

### Prerequisites
```bash
pip install pandas numpy scikit-learn xgboost lightgbm sqlalchemy pyodbc
```

Drop your `sql_loader.py` into the project root (it's already shaped correctly).

### Train + score
```bash
cd mlb_strikeout_model
python pipeline.py
```

What you'll see:
- Logs to `logs/mlb_pipeline.log` and stdout
- Models saved to `./artifacts/` (XGB JSON, LGBM text, sklearn pickles)
- Four SQL Server tables populated (created if missing)

The first run takes the longest because trees train from scratch. Subsequent runs (after a few thousand more matchups land in the source table) re-train from the new data.

### Place bets

1. **Pull tonight's slate** from `fact_pitcher_strikeout_betting_ev` for the current `game_date`.
2. **Fill in `sportsbook`, `line`, `over_odds`, `under_odds`** in those rows. Decimal odds, e.g. `1.90`. The four columns can be filled however you like — manual entry, an Azure Data Factory copy from a sportsbook scraper, etc.
3. **Run the EV calculator:**
   ```bash
   sqlcmd -S localhost -d mlb -i sql/compute_ev.sql
   ```
4. **Read the results query** at the bottom of `compute_ev.sql`. It shows pitcher, line, model probability, sportsbook implied probability, edge, EV, and a Kelly stake suggestion.

---

## How to read the output tables

### `fact_pitcher_game_strikeout_predictions`

This is your daily betting board.

| Column | Meaning |
|---|---|
| `predicted_strikeouts` | Expected total Ks (mean of the distribution). |
| `predicted_k_stddev` | Standard deviation. Higher = more variance, less confident bet. |
| `most_likely_k` | The single most-probable K count. |
| `most_likely_k_prob` | How likely that exact count is. |
| `prob_over_5_5` | P(pitcher records ≥ 6 Ks). Compare against book's implied probability for over 5.5. |
| `actual_strikeouts` | Actual Ks (NULL until the game is played). |

### `fact_pitcher_strikeout_betting_ev`

| Column | Filled by | Meaning |
|---|---|---|
| `line`, `over_odds`, `under_odds`, `sportsbook` | You | Sportsbook offering |
| `model_prob_over` | EV script | Model's P(K > line) for the matching line |
| `implied_prob_over` | EV script | 1 / over_odds |
| `edge_over` | EV script | model_prob_over − implied_prob_over |
| `ev_over` | EV script | Expected return on $1 OVER bet |
| `recommended_side` | EV script | OVER, UNDER, or PASS (based on EV > 5%) |
| `kelly_fraction` | EV script | Quarter-Kelly stake suggestion as fraction of bankroll |
| `bet_result` | EV script | WIN / LOSS / PUSH after the game |

### `fact_model_evaluation_metrics`

Every run logs metrics here. Watch these over time — if Brier or MAE start drifting upwards, the model's stale and needs retraining.

---

## What "good" looks like for the metrics

| Metric | Meaning | Good range |
|---|---|---|
| **ROC AUC** (per-PA) | Discrimination — can the model separate K from non-K? | 0.66–0.72 is typical for K prediction |
| **Brier score** | Calibration quality (lower = better) | 0.16–0.18 |
| **Log Loss** | Penalises overconfident wrong predictions | 0.50–0.55 |
| **MAE** (game-level) | Average miss in total Ks | 1.3–1.7 Ks per game |
| **Bias** (game-level) | Systematic over/under prediction. Should be ~0 | -0.2 to +0.2 |

If your AUC is much higher than 0.72, **be suspicious of leakage** — go check `LEAKAGE_COLS` in `config.py` and confirm nothing post-event is sneaking in.

---

## Important caveats — read before betting real money

1. **Calibration drift is real.** Sportsbooks adjust lines based on news (injuries, weather, lineup changes). Your model doesn't see that. If the line moves significantly between when you score the game and when you bet, your edge may have evaporated.

2. **The 5% EV threshold isn't magic.** It's a defensive buffer because (a) the model has its own error, (b) the book has a vig, and (c) sharp money will move soft lines. Lower it at your own risk.

3. **Quarter-Kelly is conservative for a reason.** Full Kelly is only optimal if your probability estimates are *exactly* right. They're not. Quarter-Kelly cuts your variance dramatically while still capturing most of the long-run growth. Resist the urge to bet bigger.

4. **The model assumes lineups are known.** If you score a game before the lineup is posted, you're using last-known-lineup features, which can be wrong. Consider re-scoring after lineups drop, especially for key bench guys.

5. **Sample weights help with mixed-PA rows.** The source view has some rows representing single PAs and some representing multi-PA aggregates (when `hitter_plate_appearances > 1`). The `sample_weight` column scales their influence. The pipeline uses it automatically.

6. **Future seasons need retraining.** When 2026 wraps, roll the splits forward: train on 2024+2025, validate on early 2026, test on the rest. Edit `config.py`.

7. **This is not financial advice.** Sports betting is gambling. Even a positive-EV system loses on most individual bets and needs hundreds of bets and disciplined bankroll management to realise the edge. Don't bet what you can't afford to lose.

---

## Things worth adding next (in priority order)

1. **Park factors** — strikeouts vary noticeably by ballpark. Adding `park_id` as a categorical or merging in pitcher-friendly factor coefficients would tighten predictions.
2. **Weather** — wind direction, game-time temperature affect K rate slightly. APIs available.
3. **Umpire** — strike zone size varies meaningfully across umpires. Statcast publishes per-ump zone data.
4. **Closing line value tracking** — log the line you bet at and the closing line. If your bets consistently beat the close, you have real edge. If not, you're getting lucky.
5. **A reliever model** — currently we only score starters for game totals. Reliever K-rates are different and could be modelled separately if you want to bet "team total Ks" markets.
6. **SHAP values** — per-prediction feature attribution would let you sanity-check why the model thinks a pitcher will dominate or get rocked. Worth running for high-stake picks.

---

## Glossary

| Term | Meaning |
|---|---|
| PA | Plate Appearance — one batter facing one pitcher, ends in a strikeout, walk, hit, out, etc. |
| K | Strikeout |
| Calibration | When a model says 30%, the event happens 30% of the time |
| AUC | Area Under ROC Curve — model's ability to rank Ks above non-Ks |
| Brier score | Mean squared error of probabilities vs binary outcomes |
| EV | Expected Value — average return on a $1 bet over many trials |
| Kelly criterion | Bet-sizing formula maximising long-run bankroll growth |
| Vig / juice | The sportsbook's built-in margin (the reason 1.90/1.90 isn't a fair coin flip) |
| Poisson-binomial | Distribution of the sum of independent non-identical Bernoullis (= total Ks given each batter has different P(K)) |

---

*Built for personal research. No warranty, no guarantees, gamble responsibly.*
