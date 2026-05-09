"""
model_trainer.py
Author    : R04 - Eric (EDA & ML Engineer)
Purpose   : Trains XGBoost classifier (onset_occurred) and regressor
            (onset_doy) with TimeSeriesSplit, SHAP feature importance.
            Saves models to models/ and evaluation report to models/evaluation_report.json.
Milestone : M4 - ML Model Training
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from src.config import DATA_DIR, MODEL_DIR, DOCS_DIR, setup_logging

FEATURES_PARQUET = DATA_DIR / "features" / "onset_features.parquet"

FEATURE_COLS = [
    "mean_precip_30d",
    "precip_anomaly",
    "soil_moisture_proxy",
    "temp_anomaly_tmax",
]
TARGET_CLF = "onset_occurred"
TARGET_REG = "onset_doy"


def _load_or_generate() -> pd.DataFrame:
    if FEATURES_PARQUET.exists():
        logger.info("Loading features from %s", FEATURES_PARQUET)
        return pd.read_parquet(FEATURES_PARQUET)
    logger.warning("Features parquet not found - running feature_engineer.py ...")
    from src.ml.feature_engineer import main as fe_main
    return fe_main()


def train_models(df: pd.DataFrame) -> dict:
    """Train classifier and regressor with TimeSeriesSplit. Returns eval report."""
    try:
        from xgboost import XGBClassifier, XGBRegressor
        from sklearn.model_selection import TimeSeriesSplit, RandomizedSearchCV
        from sklearn.preprocessing import StandardScaler
        from sklearn.pipeline import Pipeline
        from sklearn.metrics import roc_auc_score, mean_absolute_error
        import joblib
    except ImportError as exc:
        raise ImportError(f"Required packages missing: {exc}. pip install xgboost scikit-learn joblib") from exc

    df = df.dropna(subset=FEATURE_COLS).copy()
    df = df.sort_values(["year", "season"])

    X = df[FEATURE_COLS].values
    y_clf = df[TARGET_CLF].astype(int).values
    y_reg = df[TARGET_REG].fillna(df[TARGET_REG].median()).values

    tscv = TimeSeriesSplit(n_splits=5)

    param_dist = {
        "clf__n_estimators":  [50, 100, 200],
        "clf__max_depth":     [3, 5, 7],
        "clf__learning_rate": [0.05, 0.1, 0.2],
        "clf__subsample":     [0.7, 0.9, 1.0],
    }

    # ── Classifier ─────────────────────────────────────────────────────────
    clf_pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("clf",    XGBClassifier(eval_metric="logloss", random_state=42, verbosity=0)),
    ])
    clf_search = RandomizedSearchCV(
        clf_pipe, param_dist, n_iter=10, cv=tscv, scoring="roc_auc",
        n_jobs=-1, random_state=42, verbose=0,
    )
    clf_search.fit(X, y_clf)
    best_clf = clf_search.best_estimator_
    y_pred_clf = best_clf.predict_proba(X)[:, 1]
    auc = float(roc_auc_score(y_clf, y_pred_clf))
    logger.info("Classifier AUC-ROC: %.4f", auc)

    # ── Regressor ──────────────────────────────────────────────────────────
    reg_param_dist = {
        "reg__n_estimators":  [50, 100, 200],
        "reg__max_depth":     [3, 5, 7],
        "reg__learning_rate": [0.05, 0.1, 0.2],
    }
    reg_pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("reg",    XGBRegressor(random_state=42, verbosity=0)),
    ])
    reg_search = RandomizedSearchCV(
        reg_pipe, reg_param_dist, n_iter=10, cv=tscv, scoring="neg_mean_absolute_error",
        n_jobs=-1, random_state=42, verbose=0,
    )
    reg_search.fit(X, y_reg)
    best_reg = reg_search.best_estimator_
    y_pred_reg = best_reg.predict(X)
    mae  = float(mean_absolute_error(y_reg, y_pred_reg))
    rmse = float(np.sqrt(np.mean((y_reg - y_pred_reg) ** 2)))
    logger.info("Regressor MAE: %.2f days | RMSE: %.2f days", mae, rmse)

    # ── Save models ────────────────────────────────────────────────────────
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    clf_path = MODEL_DIR / "onset_classifier.pkl"
    reg_path = MODEL_DIR / "onset_regressor.pkl"
    joblib.dump(best_clf, clf_path)
    joblib.dump(best_reg, reg_path)
    logger.info("Saved classifier -> %s", clf_path)
    logger.info("Saved regressor  -> %s", reg_path)

    # ── SHAP importance ────────────────────────────────────────────────────
    try:
        import shap
        explainer = shap.TreeExplainer(best_clf.named_steps["clf"])
        scaler = best_clf.named_steps["scaler"]
        shap_vals = explainer.shap_values(scaler.transform(X))
        if isinstance(shap_vals, list):
            shap_vals = shap_vals[1]
        importance = dict(zip(FEATURE_COLS, np.abs(shap_vals).mean(axis=0).tolist()))
        # Try to save plot; fall back to text
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            shap.summary_plot(shap_vals, X, feature_names=FEATURE_COLS, show=False)
            DOCS_DIR.mkdir(parents=True, exist_ok=True)
            plt.savefig(DOCS_DIR / "shap_importance.png", bbox_inches="tight")
            plt.close()
            logger.info("SHAP plot saved -> %s", DOCS_DIR / "shap_importance.png")
        except Exception as plot_err:
            logger.warning("Could not save SHAP plot: %s", plot_err)
            DOCS_DIR.mkdir(parents=True, exist_ok=True)
            with open(DOCS_DIR / "shap_importance.txt", "w") as f:
                for feat, val in sorted(importance.items(), key=lambda x: -x[1]):
                    f.write(f"{feat}: {val:.4f}\n")
    except ImportError:
        logger.warning("SHAP not installed - skipping feature importance plot.")
        importance = {}

    report = {
        "classifier": {"auc_roc": auc, "best_params": clf_search.best_params_},
        "regressor":  {"mae_days": mae, "rmse_days": rmse, "best_params": reg_search.best_params_},
        "shap_importance": importance,
        "n_training_rows": len(df),
    }
    report_path = MODEL_DIR / "evaluation_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Evaluation report -> %s", report_path)
    return report


def main() -> None:
    setup_logging("model_trainer.log")
    df = _load_or_generate()
    report = train_models(df)
    logger.info("Training complete. AUC=%.4f, MAE=%.2f days",
                report["classifier"]["auc_roc"], report["regressor"]["mae_days"])


if __name__ == "__main__":
    main()
