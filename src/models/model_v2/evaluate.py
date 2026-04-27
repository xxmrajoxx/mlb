"""
Evaluation: metrics, calibration, diagnostics, plots.
"""
import logging
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xgboost as xgb

from model_1_k_probability import config

logger = logging.getLogger(__name__)


def weighted_rmse(y_true: np.ndarray, y_pred: np.ndarray, w: np.ndarray) -> float:
    """Root mean squared error weighted by sample weights (PAs)."""
    return float(np.sqrt(np.average((y_true - y_pred) ** 2, weights=w)))


def weighted_mae(y_true: np.ndarray, y_pred: np.ndarray, w: np.ndarray) -> float:
    """Mean absolute error weighted by sample weights."""
    return float(np.average(np.abs(y_true - y_pred), weights=w))


def log_loss_approx(y_true_rate: np.ndarray, y_pred_rate: np.ndarray,
                    w: np.ndarray) -> float:
    """
    Approximate weighted log loss for a rate target, treating each PA
    as an independent Bernoulli trial.
    """
    eps = 1e-7
    y_pred_clip = np.clip(y_pred_rate, eps, 1 - eps)
    ll = -(y_true_rate * np.log(y_pred_clip) +
           (1 - y_true_rate) * np.log(1 - y_pred_clip))
    return float(np.average(ll, weights=w))


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray,
                    w: np.ndarray, label: str = "") -> Dict:
    """Compute a suite of metrics and log them."""
    # Clip predictions to [0, 1] for sanity
    y_pred_c = np.clip(y_pred, 0, 1)

    metrics = {
        "n": int(len(y_true)),
        "weighted_sum_PAs": float(w.sum()),
        "rmse": weighted_rmse(y_true, y_pred_c, w),
        "mae": weighted_mae(y_true, y_pred_c, w),
        "log_loss": log_loss_approx(y_true, y_pred_c, w),
        "mean_actual_k_rate": float(np.average(y_true, weights=w)),
        "mean_pred_k_rate": float(np.average(y_pred_c, weights=w)),
        "calibration_bias": float(
            np.average(y_pred_c, weights=w) - np.average(y_true, weights=w)
        ),
    }

    logger.info(f"=== METRICS: {label} ===")
    for k, v in metrics.items():
        if isinstance(v, float):
            logger.info(f"  {k}: {v:.6f}")
        else:
            logger.info(f"  {k}: {v}")

    return metrics


def calibration_bins(y_true: np.ndarray, y_pred: np.ndarray,
                     w: np.ndarray, n_bins: int = 10) -> pd.DataFrame:
    """
    Return a reliability table: predicted bin -> actual K rate in that bin.
    Perfect calibration means predicted == actual in every bin.
    """
    df = pd.DataFrame({"y_pred": np.clip(y_pred, 0, 1),
                       "y_true": y_true, "w": w})
    df["bin"] = pd.qcut(df["y_pred"], q=n_bins, duplicates="drop",
                        labels=False)

    g = df.groupby("bin")
    table = pd.DataFrame({
        "bin_lower": g["y_pred"].min(),
        "bin_upper": g["y_pred"].max(),
        "mean_predicted": g.apply(lambda d: np.average(d["y_pred"], weights=d["w"])),
        "mean_actual": g.apply(lambda d: np.average(d["y_true"], weights=d["w"])),
        "total_PAs": g["w"].sum(),
        "n_matchups": g.size(),
    }).reset_index()

    table["abs_miscalibration"] = (table["mean_predicted"] - table["mean_actual"]).abs()
    return table


def plot_calibration(cal_table: pd.DataFrame, out_path: Path,
                     title: str = "Calibration") -> None:
    """Plot predicted vs actual K rate by decile."""
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.plot([0, 0.6], [0, 0.6], linestyle="--", color="gray",
            label="Perfect calibration")
    ax.scatter(cal_table["mean_predicted"], cal_table["mean_actual"],
               s=cal_table["total_PAs"] / cal_table["total_PAs"].max() * 300,
               alpha=0.7, label="Predicted deciles")
    for _, row in cal_table.iterrows():
        ax.annotate(f"{int(row['n_matchups'])}",
                    (row["mean_predicted"], row["mean_actual"]),
                    fontsize=8, xytext=(3, 3), textcoords="offset points")
    ax.set_xlabel("Mean predicted K rate (within bin)")
    ax.set_ylabel("Mean actual K rate (within bin)")
    ax.set_title(title)
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    logger.info(f"Saved calibration plot to {out_path}")


def plot_shap_top_features(shap_df: pd.DataFrame, out_path: Path,
                           top_n: int = 30) -> None:
    """Horizontal bar chart of top-N features by mean |SHAP|."""
    top = shap_df.head(top_n).iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, max(6, top_n * 0.25)))
    ax.barh(top["feature"], top["mean_abs_shap"])
    ax.set_xlabel("Mean |SHAP value|")
    ax.set_title(f"Top {top_n} features by SHAP importance")
    ax.grid(alpha=0.3, axis="x")
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    logger.info(f"Saved SHAP importance plot to {out_path}")


def evaluate_by_subgroup(df: pd.DataFrame, y_true_col: str,
                         y_pred_col: str, weight_col: str,
                         group_cols: List[str]) -> pd.DataFrame:
    """
    Break down model performance by subgroup (e.g. starter vs reliever,
    month, handedness matchup).
    """
    rows = []
    for group in group_cols:
        if group not in df.columns:
            continue
        for val, sub in df.groupby(group):
            if len(sub) < 100:
                continue
            rmse = weighted_rmse(
                sub[y_true_col].values, sub[y_pred_col].values, sub[weight_col].values
            )
            rows.append({
                "group": group,
                "value": val,
                "n": len(sub),
                "rmse": rmse,
                "mean_actual": np.average(sub[y_true_col], weights=sub[weight_col]),
                "mean_pred": np.average(sub[y_pred_col], weights=sub[weight_col]),
            })
    return pd.DataFrame(rows)


def predict_with_uncertainty(models: List[xgb.Booster],
                             X: pd.DataFrame) -> pd.DataFrame:
    """
    Generate predictions from multiple bootstrap models; return mean and std.
    """
    dm = xgb.DMatrix(X)
    preds = np.stack([m.predict(dm) for m in models], axis=0)
    return pd.DataFrame({
        "predicted_k_rate": preds.mean(axis=0),
        "prediction_std": preds.std(axis=0),
    }, index=X.index)
