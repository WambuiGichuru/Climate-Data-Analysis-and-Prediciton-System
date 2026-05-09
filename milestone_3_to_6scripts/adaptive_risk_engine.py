import joblib
import numpy as np

def calculate_nairobi_risk(live_precip_72h, prophet_forecast, xgboost_pred):
    """
    R-04 Innovation: Adaptive Weighting Logic
    Combines Batch (Prophet), Speed (Rule-based), and ML (XGBoost)
    """
    # 1. Rule-based signal (20% weight) - Speed Layer
    rule_score = 1.0 if live_precip_72h >= 20.0 else 0.0
    
    # 2. XGBoost signal (40% weight) - Reactive ML
    # 3. Prophet signal (40% weight) - Seasonal Baseline
    
    # Final weighted risk calculation
    final_risk_score = (0.2 * rule_score) + (0.4 * xgboost_pred) + (0.4 * prophet_forecast)
    
    return {
        "score": final_risk_score,
        "level": "HIGH" if final_risk_score > 0.7 else "MODERATE" if final_risk_score > 0.4 else "LOW"
    }

# Example use-case for the Technical Report
print(f"Final Risk Assessment: {calculate_nairobi_risk(25.0, 0.6, 0.8)}")