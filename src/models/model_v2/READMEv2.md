# Model 1: Per-Matchup Strikeout Probability

Trains an XGBoost model to predict the probability that a hitter strikes out per plate appearance against a given pitcher. Output feeds downstream simulation and EV calculation.

## Quick start

```bash
# From the project root
python -m model_1_k_probability.run_pipeline

# Force refresh data from SQL Server (bypass cache)
python -m model_1_k_probability.run_pipeline --force-refresh

# Custom number of Optuna trials
python -m model_1_k_probability.run_pipeline --trials 200
```

## Pipeline steps

1. **Load** matchup feature data from `mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2` (cached locally as parquet).
2. **Preprocess** — select numeric features, drop identifiers/labels/suspected-leakage columns, filter training rows.
3. **Chronological split** — 2023 train / 2024 val / 2025 test / 2026 inference.
4. **Optuna tuning** — 100 trials (configurable) on validation RMSE.
5. **Initial model** — trained on full feature set.
6. **SHAP pruning** — keep top-80 features by mean-absolute-SHAP importance.
7. **Retrain** — fresh tune on pruned features, then fit final model.
8. **Bootstrap models** — 5 models on resampled data for prediction uncertainty.
9. **Evaluate** — test RMSE, calibration table, calibration plot, subgroup breakdown.
10. **Predict** — score every row across all seasons including 2026 inference.
11. **Export** — write predictions to `mlb.dbo.fact_model_1_k_probability_predictions` in SQL Server + CSV backup.

## Output table: `fact_model_1_k_probability_predictions`

Key columns include:

- **Identity**: gamePk, game_date, season, hitter/pitcher IDs and names, team names
- **Context**: pitcher_is_starter, hitter_batting_order, hitter_lineup_position
- **Predictions**: predicted_k_rate, prediction_std, predicted_k_count_per_matchup, expected_plate_appearances, predicted_pitcher_total_ks, prediction_confidence_tier
- **Actuals** (where known): actual_k_count, actual_plate_appearances, actual_k_rate, actual_pitcher_total_ks, prediction_error
- **Key features** for post-hoc analysis: hitter/pitcher rolling K rates, whiff rates, pitch volume, BF, IP, H2H history

## Artifacts

Each run produces `artifacts/model_1_run_<timestamp>/` containing:

- `model.json` — the main XGBoost model
- `bootstrap_model_[0-4].json` — uncertainty ensemble
- `features.json` — feature list used
- `hyperparameters.json` — best Optuna params
- `shap_importance.csv` — full SHAP ranking
- `metadata.json` — run config + metrics
- Plots in `plots/`: `shap_importance_*.png`, `calibration_test_*.png`

## Configuration

All knobs are in `config.py`:

- `TRAIN_SEASONS` / `VAL_SEASONS` / `TEST_SEASONS` / `INFERENCE_SEASONS`
- `N_OPTUNA_TRIALS` (default 100)
- `TOP_N_FEATURES_AFTER_SHAP` (default 80)
- `N_BOOTSTRAP_MODELS` (default 5)
- `XGB_OBJECTIVE` (default `reg:squarederror`; `reg:logistic` is also worth trying)

## Adapting to your SQL loader

The `data_loader.py` and `export.py` modules import from `sql.sql_loader`. If your module names differ, edit the imports at the top of those two files. Specifically:

- `data_loader.py` needs a `execute_query(sql) -> DataFrame` function
- `export.py` needs either a `write_dataframe(df, table_name, if_exists)` function or both `execute_sql(sql)` and `insert_many(sql, rows)`

## IMPORTANT — before real money

1. **Paper trade for a full season.** Log predictions vs. actuals vs. closing lines. Track CLV (closing line value).
2. **Only bet where your edge exceeds ~3%.** The vig eats small edges over time.
3. **Use Kelly fractional sizing**, not flat bets. Full Kelly is too volatile for beginners — start at quarter-Kelly.
4. **Track every bet.** If your actual ROI doesn't match predicted ROI over 500+ bets, the model is miscalibrated.
