"""
Evaluation metrics for the strikeout model.

We track classification metrics (per-PA P(K)) AND regression metrics
(game-level total Ks). Both matter - the per-PA model needs to be well
calibrated, and the game-level prediction needs to have low MAE so over/under
betting EV is reliable.
"""

import logging
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score, roc_auc_score, log_loss,
    precision_score, recall_score, f1_score, brier_score_loss,
    classification_report, mean_absolute_error, mean_squared_error,
)

logger = logging.getLogger(__name__)


def evaluate_classifier(y_true, y_prob, threshold: float = 0.5, label: str = "model"):
    """Classification metrics for per-PA strikeout probability.

    y_true may be fractional (per-hitter K rate when a row represents
    multiple PAs). Most classification metrics need binary inputs, so we
    binarise y_true at threshold for those metrics. Brier and log loss
    work with fractional labels and use the raw value directly - which is
    actually a more honest measure for our use case.
    """
    y_true = np.asarray(y_true, dtype=float)
    y_prob = np.asarray(y_prob, dtype=float)
    y_pred = (y_prob >= threshold).astype(int)
    y_true_bin = (y_true >= threshold).astype(int)  # for binary-only metrics

    # Manual log loss that accepts fractional y_true
    p_clip = np.clip(y_prob, 1e-15, 1 - 1e-15)
    manual_log_loss = float(-np.mean(y_true * np.log(p_clip) + (1 - y_true) * np.log(1 - p_clip)))

    metrics = {
        "label": label,
        "n": int(len(y_true)),
        "accuracy": float(accuracy_score(y_true_bin, y_pred)),
        "roc_auc": (float(roc_auc_score(y_true_bin, y_prob))
                    if len(np.unique(y_true_bin)) > 1 else np.nan),
        "log_loss": manual_log_loss,
        "brier": float(np.mean((y_prob - y_true) ** 2)),
        "precision": float(precision_score(y_true_bin, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true_bin, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true_bin, y_pred, zero_division=0)),
        "base_rate": float(y_true.mean()),  # mean K rate (fractional)
        "pred_rate": float(y_prob.mean()),
    }

    logger.info(
        f"[{label}] n={metrics['n']:,}  AUC={metrics['roc_auc']:.4f}  "
        f"LogLoss={metrics['log_loss']:.4f}  Brier={metrics['brier']:.4f}  "
        f"Acc={metrics['accuracy']:.4f}  "
        f"BaseRate={metrics['base_rate']:.3f}  PredRate={metrics['pred_rate']:.3f}"
    )
    return metrics


def evaluate_game_total(game_df: pd.DataFrame, label: str = "game"):
    """
    Game-level evaluation: how close are predicted total Ks to actual?

    Expects game_df to have columns:
      - actual_strikeouts
      - predicted_strikeouts
    """
    y_true = game_df["actual_strikeouts"].astype(float)
    y_pred = game_df["predicted_strikeouts"].astype(float)

    metrics = {
        "label": label,
        "n_games": int(len(game_df)),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "bias": float((y_pred - y_true).mean()),  # positive = over-predicting
        "actual_mean": float(y_true.mean()),
        "predicted_mean": float(y_pred.mean()),
    }

    logger.info(
        f"[{label}] games={metrics['n_games']:,}  "
        f"MAE={metrics['mae']:.3f}  RMSE={metrics['rmse']:.3f}  "
        f"Bias={metrics['bias']:+.3f}  "
        f"Actual={metrics['actual_mean']:.2f}  Predicted={metrics['predicted_mean']:.2f}"
    )
    return metrics
