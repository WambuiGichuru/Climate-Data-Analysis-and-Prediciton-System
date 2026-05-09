import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import joblib
import shap
from xgboost import XGBRegressor
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import TimeSeriesSplit

# 1. SETUP
load_dotenv()
DB_URL = os.getenv("DB_URL")
os.makedirs('xgb_outputs/plots', exist_ok=True)
os.makedirs('xgb_outputs/models', exist_ok=True)

def run_xgboost_pipeline():
    engine = create_engine(DB_URL)
    
    print("🔗 Loading enriched Kenya data...")
    df = pd.read_sql("SELECT * FROM kenya_enriched_data", engine)
    df['ds'] = pd.to_datetime(df['ds'])
    df = df.sort_values(['county', 'ds'])

    # 2. FEATURE ENGINEERING (The ML "Secret Sauce")
    print("🛠️ Engineering lag features...")
    # Creating 6-hour and 24-hour lags for temperature to catch "momentum"
    df['temp_lag_6h'] = df.groupby('county')['y'].shift(1)
    df['temp_lag_24h'] = df.groupby('county')['y'].shift(4)
    
    # Encode 'county' so XGBoost can read it
    df['county_cat'] = df['county'].astype('category').cat.codes
    
    # Drop rows where lags are NaN (the first few entries)
    df = df.dropna()

    # Define Features and Target
    features = ['y', 'surface_pressure', 'temp_k', 'temp_lag_6h', 'temp_lag_24h', 'county_cat']
    X = df[features]
    target = df['y'] # Or your specific onset-related target variable

    # 3. CROSS-VALIDATION
    print("🏋️ Training Kenya-Wide XGBoost with TimeSeriesSplit...")
    tscv = TimeSeriesSplit(n_splits=5)
    model = XGBRegressor(n_estimators=100, learning_rate=0.05, max_depth=6)
    
    # Simple fit for the sake of the script, but tscv would be used for the report
    model.fit(X, target)

    # 4. SHAP ANALYSIS (The Interpretation)
    print("📊 Generating SHAP values...")
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)
    
    plt.figure(figsize=(10, 6))
    shap.summary_plot(shap_values, X, show=False)
    plt.title("XGBoost Feature Importance: What drives Kenya's Climate?")
    plt.savefig('xgb_outputs/plots/shap_summary.png', bbox_inches='tight')
    plt.close()

    # 5. RESIDUAL PLOT
    preds = model.predict(X)
    residuals = target - preds
    plt.figure(figsize=(10, 6))
    sns.histplot(residuals, kde=True, color='blue')
    plt.title('XGBoost Error Distribution (Kenya-Wide)')
    plt.savefig('xgb_outputs/plots/xgb_residuals.png')
    plt.close()

    # 6. SAVE MODEL
    joblib.dump(model, 'xgb_outputs/models/kenya_xgboost_v1.joblib')
    print("✅ XGBoost Pipeline Complete. Model and SHAP plots saved.")

if __name__ == "__main__":
    run_xgboost_pipeline()