# MLB Hitter Strikeout Classifier — Progress Summary

Date: 2026-04-21

## Goal
Build a production-style Python pipeline for a hitter strikeout model using XGBoost classification.

Core idea:
- 1 row = hitter vs pitcher
- target = whether the hitter recorded at least 1 strikeout
- model output = probability the hitter strikes out
- later aggregate hitter probabilities to estimate pitcher expected strikeouts

---

## What we completed today

### 1. Locked the first model objective
We confirmed the first model is a **classifier** and not a regressor.

Current target logic:
```python
(df["hitter_strikeOuts"] > 0).astype(int)
```

Meaning:
- `0` = hitter did not strike out
- `1` = hitter got at least 1 strikeout

Important note:
- this model does **not** distinguish between 1 strikeout and 2+ strikeouts
- it only predicts `0` vs `1+`

---

### 2. Decided on production `.py` file structure
We moved away from the notebook workflow and started building a production-style Python script.

Main functions created/planned:
- `load_model_data()`
- `prepare_features(df)`
- `split_by_season(...)`
- `encode_train_test(...)`
- `train_model(...)`
- `evaluate_model(...)`
- `save_results_to_sql(...)` (planned / in progress)

---

### 3. Reused shared SQL connection code
We agreed to reuse your existing `sql_loader.py` instead of duplicating database connection logic.

Shared helper file includes:
- `get_engine()`
- `load_dataframe()`
- `truncate_table()`
- `execute_sql()`

---

### 4. Confirmed realistic train/test methodology
We confirmed the first production split should be:
- **train = 2025**
- **test = 2026**

Reason:
- this simulates real-world prediction much better than a random split
- train on past season
- test on future unseen season

Important note:
- the model can still use 2026 **features** for prediction at inference time
- but we should not train on 2026 outcomes if 2026 is the holdout test season

---

### 5. Kept metadata separate from model features
We agreed it was important to preserve non-model columns separately for analysis and later joins.

Examples:
- `hitter_name`
- `pitcher_name`
- `hitter_team_name`
- `pitcher_team_name`
- `gamePk`
- `game_date`

Reason:
- needed for reviewing outputs
- needed for betting-style analysis
- needed for later aggregation to pitcher level

---

### 6. Removed leakage / non-model columns
We identified and removed obvious leakage / same-game outcome columns from the feature matrix.

Examples removed:
- `pitches_seen_vs_pitcher`
- `swings_vs_pitcher`
- `whiffs_vs_pitcher`
- `called_strikes_vs_pitcher`
- `matchup_whiff_rate`
- `matchup_called_strike_rate`
- `matchup_csw_rate`
- `hitter_game_plate_appearances`
- `hitter_game_strikeouts`

Also removed / excluded metadata and direct target columns from `X`.

---

### 7. Changed encoding approach
We discussed two encoding options:
- `LabelEncoder`
- `pd.get_dummies()`

We decided to use **one-hot encoding (`pd.get_dummies`)** instead of `LabelEncoder`.

Reason:
- `LabelEncoder` introduces fake ordering into categorical variables
- one-hot encoding is more appropriate for fields like:
  - `hitter_position`
  - `pitcher_throws`
  - `hitter_stand`
  - `hitter_lineup_position`

---

### 8. Chose split-first, then encode
We corrected the workflow to:
1. load data
2. prepare features
3. split by season
4. encode train/test after split
5. align test columns to training columns

Reason:
- avoids leaking information from the test set into preprocessing
- is closer to proper production methodology

---

### 9. Built first XGBoost classifier workflow
The current model setup is:
- `XGBClassifier`
- target = 1+ strikeout
- metrics used:
  - Accuracy
  - ROC AUC
  - Log Loss

Current first run results:
- **Accuracy:** 0.6868
- **ROC AUC:** 0.6333
- **Log Loss:** 0.5998

Interpretation:
- better than random
- useful first baseline
- not strong enough yet to trust for real betting decisions

---

### 10. Clarified meaning of current output
Current row-level output gives:
- actual hitter strikeout result
- binary target
- predicted binary target
- predicted probability of **1+ strikeout**

Important clarification:
- this output is **not** predicting 2+ strikeouts
- it only predicts whether the hitter gets at least 1 strikeout

---

### 11. Clarified why MAE / RMSE were used yesterday
We discussed why MAE / RMSE appeared in the earlier workflow.

Explanation:
- the **classifier** is evaluated at hitter-row level using classification metrics
- after summing hitter strikeout probabilities by pitcher, we get **expected pitcher strikeouts**
- once we compare expected pitcher strikeouts to actual pitcher strikeouts, that becomes a continuous prediction problem

That is why the pitcher-level evaluation uses:
- `MAE`
- `RMSE`

---

### 12. Drafted SQL table structure for hitter prediction output
We created a draft SQL Server table design for:
- `mlb.dbo.model_hitter_strikeout_predictions`

Planned columns included:
- game info
- hitter info
- pitcher info
- actuals
- model outputs
- audit timestamp

---

## Issues / discoveries from today

### 1. SQL insert failed because of extra column
The Python output still included:
- `target_hitter_k`

But the SQL table did not have that column.

Error:
- `Invalid column name 'target_hitter_k'`

Reason:
- `meta_df` still included the model target column
- later the results dataframe inherited it and tried to load it into SQL Server

Fix identified:
- keep only `hitter_strikeOuts` in `meta_df`
- do **not** carry `target_hitter_k` into the SQL output dataframe

---

### 2. SQL loading helper currently hides failure
In `sql_loader.py`, `load_dataframe()` logs the exception but does **not** raise it.

Result:
- the pipeline can log a failure
- but still continue and print misleading success messages

Fix identified:
- add `raise` inside the `except` block so failures stop the run cleanly

---

### 3. Truncate behavior needs review
We discussed removing `truncate_table()` from the save step because you want to preserve daily history.

Important consequence:
- if append mode is used without dedupe logic, reruns may create duplicates

Longer-term options:
- use a staging table + `MERGE`
- add unique key logic
- or append with a run date / load timestamp and manage history intentionally

---

## Still required to do

### 1. Finish SQL output loading properly
This is still incomplete and needs to be done tomorrow.

Required fixes:
- remove `target_hitter_k` from `meta_df`
- update `save_results_to_sql()`
- decide whether output should:
  - append daily results, or
  - deduplicate / merge
- update `sql_loader.py` so load errors raise properly

This is one of the biggest unfinished pieces.

---

### 2. Finalise loading model results into SQL Server table
The end-to-end save flow is **not complete yet**.

Still required:
- confirm final schema for `mlb.dbo.model_hitter_strikeout_predictions`
- confirm Python output columns match SQL table exactly
- rerun load successfully
- validate row counts in SQL Server
- confirm sort / review query in SQL Server

---

### 3. Decide daily history strategy
Need to decide how model outputs should be stored over time.

Options:
- append all daily runs
- replace latest run only
- keep history with `load_ts`
- create unique constraint and upsert only one row per `gamePk + hitter_id + pitcher_id`

This needs to be agreed before productionising the save logic.

---

### 4. Build pitcher aggregation step
This is still to do.

Goal:
- sum hitter strikeout probabilities to estimate pitcher expected strikeouts

Planned logic:
- group by pitcher/game
- sum `predicted_probability`
- compare against actual pitcher strikeouts

---

### 5. Create pitcher-level SQL output table
Still to do:
- create `mlb.dbo.model_pitcher_expected_strikeouts`
- decide final schema
- load aggregated pitcher results into SQL Server

---

### 6. Add pitcher-level evaluation
Still to do:
- calculate `MAE`
- calculate `RMSE`
- review whether aggregated expected strikeouts are useful

---

### 7. Review output sorting / usability
You requested sorting by:
- team name
- pitcher name
- game date

Need to confirm whether that sort should happen:
- in Python before saving
- in SQL when reading
- or both

---

### 8. Improve model evaluation
Current first-run metrics are only the start.

Still to do:
- review precision / recall
- review calibration
- possibly inspect false positives / false negatives
- review threshold choice instead of blindly using 0.50

---

### 9. Consider follow-up models
Not required immediately, but identified as future work:
- 2+ hitter strikeout classifier
- `XGBRegressor` for expected hitter strikeouts

These were discussed but not built yet.

---

### 10. Model saving / reuse
We discussed `joblib`, but have not implemented it.

Still to do:
- save trained model to disk
- later reuse trained model without retraining every run

---

## Recommended next steps for tomorrow

Suggested order:

1. Fix `meta_df` so `target_hitter_k` is not sent to SQL output
2. Fix `sql_loader.py` to raise on load failure
3. Finalise `save_results_to_sql()`
4. Successfully load hitter predictions into SQL Server
5. Validate table contents in SQL Server
6. Build pitcher aggregation step
7. Save pitcher expected strikeouts to SQL Server
8. Add pitcher-level MAE / RMSE

---

## Current overall status

### Working
- classifier target decision
- feature prep direction
- leakage removal direction
- split-by-season methodology
- post-split encoding direction
- first XGBoost classifier run
- baseline metrics

### Partially done
- production script structure
- SQL table design
- SQL output flow

### Not finished
- reliable SQL save/load for model outputs
- pitcher aggregation
- pitcher-level evaluation
- daily-history strategy
- full productionisation / automation

---

## Quick reminder for tomorrow
The biggest unfinished item is:

> **Loading the model output into SQL Server cleanly and correctly**

That includes:
- fixing the dataframe columns
- fixing the loader error handling
- deciding append vs merge behavior
- validating the final SQL table output

