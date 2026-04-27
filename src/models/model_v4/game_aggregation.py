"""
Aggregate per-PA strikeout probabilities into pitcher-game totals.

For a starting pitcher, total Ks = sum over each batter faced of P(K | matchup).
This is the *expected value* of a Poisson-binomial random variable. If we
need a probability distribution over total Ks (e.g. for over 5.5 strikeouts
betting), we can either:
  (a) compute the Poisson-binomial PMF analytically, or
  (b) approximate with a Normal using mean = sum(p), var = sum(p*(1-p))

We do (a) here because PA counts are small (typically 18-28) so the exact
PMF is cheap and gives strictly better tails than the Normal approximation.
"""

import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def poisson_binomial_pmf(probs: np.ndarray) -> np.ndarray:
    """
    Exact PMF of a Poisson-binomial: probability of getting 0, 1, 2, ..., n successes
    given n independent Bernoulli trials with probabilities `probs`.

    Uses the standard convolution algorithm. O(n^2) which is fine for n ~ 30.
    """
    n = len(probs)
    pmf = np.zeros(n + 1)
    pmf[0] = 1.0
    for p in probs:
        new_pmf = np.zeros(n + 1)
        new_pmf[0] = pmf[0] * (1 - p)
        for k in range(1, n + 1):
            new_pmf[k] = pmf[k - 1] * p + pmf[k] * (1 - p)
        pmf = new_pmf
    return pmf


def aggregate_to_pitcher_game(pa_df: pd.DataFrame) -> pd.DataFrame:
    """
    Group per-PA predictions by pitcher-game and compute total Ks expectations.

    Parameters
    ----------
    pa_df : DataFrame with at least these columns:
        gamePk, game_date, season,
        pitcher_id, pitcher_name, pitcher_team_id, pitcher_team_name,
        hitter_team_id, hitter_team_name,
        pitcher_is_starter,
        prob_strikeout (calibrated per-PA P(K)),
        actual_strikeout (0/1 - the truth, may be NaN for future games)

    Returns
    -------
    DataFrame keyed by (gamePk, pitcher_id) with predicted_strikeouts and
    over/under probabilities for the most common betting lines (3.5, 4.5,
    5.5, 6.5, 7.5, 8.5, 9.5).
    """
    required = {
        "gamePk", "game_date", "season",
        "pitcher_id", "pitcher_name", "pitcher_team_id", "pitcher_team_name",
        "hitter_team_id", "hitter_team_name",
        "prob_strikeout",
    }
    missing = required - set(pa_df.columns)
    if missing:
        raise ValueError(f"aggregate_to_pitcher_game missing columns: {missing}")

    # Only starters get game-level totals.
    if "pitcher_is_starter" in pa_df.columns:
        starter_df = pa_df[pa_df["pitcher_is_starter"].astype(float) == 1].copy()
    else:
        starter_df = pa_df.copy()

    logger.info(
        f"Aggregating {len(starter_df):,} starter rows into pitcher-games"
    )

    group_cols = ["gamePk", "game_date", "season",
                  "pitcher_id", "pitcher_name", "pitcher_team_id", "pitcher_team_name"]

    rows = []
    for keys, sub in starter_df.groupby(group_cols, dropna=False):
        # IMPORTANT: each row in the source view represents one HITTER (not one PA).
        # That hitter may face the pitcher multiple times in a game, captured by
        # `hitter_plate_appearances`. We expand each row into N copies of its
        # K probability, so a hitter faced 3 times contributes 3 Bernoulli trials
        # to the Poisson-binomial sum.
        per_hitter_probs = sub["prob_strikeout"].values.astype(float)
        per_hitter_probs = np.clip(per_hitter_probs, 1e-6, 1 - 1e-6)

        if "hitter_plate_appearances" in sub.columns:
            pa_counts = sub["hitter_plate_appearances"].fillna(1).astype(int).values
            # Floor at 1 - if a hitter is in the lineup at all, they had >=1 PA
            pa_counts = np.maximum(pa_counts, 1)
        else:
            pa_counts = np.ones(len(per_hitter_probs), dtype=int)

        # Expand: for each hitter, replicate their P(K) once per PA they took
        probs = np.repeat(per_hitter_probs, pa_counts)

        if len(probs) == 0:
            # Defensive: skip empty groups (shouldn't happen but won't crash)
            continue

        pmf = poisson_binomial_pmf(probs)
        ks = np.arange(len(pmf))

        expected_k = float((ks * pmf).sum())
        var_k = float(((ks - expected_k) ** 2 * pmf).sum())

        # Probability of going OVER each common line. P(K > line) = P(K >= ceil(line+epsilon))
        over_probs = {}
        for line in [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5]:
            threshold = int(np.ceil(line))  # K must be >= threshold
            over_probs[f"prob_over_{line}"] = float(pmf[threshold:].sum())

        # Most likely K count and that count's probability.
        mode_k = int(ks[np.argmax(pmf)])
        mode_prob = float(pmf.max())

        # Actual Ks for the game = sum of strikeouts across all hitters faced.
        # The source view's `hitter_strikeouts` column gives total Ks per hitter
        # in this matchup (not a 0/1 per-PA). Prefer it when available; fall
        # back to actual_strikeout (0/1) if that's all we have.
        if "hitter_strikeouts" in sub.columns and sub["hitter_strikeouts"].notna().any():
            actual_k = int(sub["hitter_strikeouts"].fillna(0).sum())
        elif "actual_strikeout" in sub.columns and sub["actual_strikeout"].notna().any():
            actual_k = int(sub["actual_strikeout"].fillna(0).sum())
        else:
            actual_k = None

        # Opponent (the team being faced).
        opp_team_id = sub["hitter_team_id"].iloc[0]
        opp_team_name = sub["hitter_team_name"].iloc[0]

        # Pitcher handedness (same for every row in the group)
        pitcher_throws = (sub["pitcher_throws"].iloc[0]
                          if "pitcher_throws" in sub.columns else None)

        # Lineup-handedness composition - counts of each batter type the
        # pitcher faces. Useful for spotting heavy-platoon-advantage games.
        if "hitter_bats" in sub.columns:
            # Each row is one HITTER, weight by their PA count to get true
            # batter-instances faced
            bats = sub["hitter_bats"].fillna("")
            opp_lhb = int(((bats == "L") * pa_counts).sum())
            opp_rhb = int(((bats == "R") * pa_counts).sum())
            opp_switch = int(((bats == "S") * pa_counts).sum())

            if pitcher_throws in ("L", "R"):
                # Switch hitters always have platoon advantage; count them
                # as opposite-side for this purpose
                opp_same = int(((bats == pitcher_throws) * pa_counts).sum())
                opp_opposite = int((((bats != pitcher_throws) & (bats != "")) * pa_counts).sum())
            else:
                opp_same = None
                opp_opposite = None
        else:
            opp_lhb = opp_rhb = opp_switch = opp_same = opp_opposite = None

        row = dict(zip(group_cols, keys))
        row.update({
            "pitcher_throws": pitcher_throws,
            "opponent_team_id": opp_team_id,
            "opponent_team_name": opp_team_name,
            "batters_faced_modeled": int(len(probs)),  # now reflects total PAs
            "opp_lhb_count": opp_lhb,
            "opp_rhb_count": opp_rhb,
            "opp_switch_count": opp_switch,
            "opp_same_side_count": opp_same,
            "opp_opposite_side_count": opp_opposite,
            "predicted_strikeouts": expected_k,
            "predicted_k_stddev": float(np.sqrt(var_k)),
            "most_likely_k": mode_k,
            "most_likely_k_prob": mode_prob,
            "actual_strikeouts": actual_k,
            **over_probs,
        })
        rows.append(row)

    out = pd.DataFrame(rows)
    logger.info(f"Built {len(out):,} pitcher-game predictions")
    return out
