"""
src.ml — Machine-learning helpers shared between the Streamlit dashboard
and the FastAPI service.

This package wraps the XGBoost regressor trained in
analysis/kenya_xgboost_model.py (artifact at
xgb_outputs/models/kenya_xgboost_v1.joblib) and exposes:

    realtime_scorer.predict_onset(county)   - dashboard-friendly score
    feature_engineer._synthetic_onset()     - offline fallback dataset
"""
