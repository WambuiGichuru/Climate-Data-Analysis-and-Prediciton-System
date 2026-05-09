import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import joblib
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Setup
load_dotenv()
DB_URL = os.getenv("DB_URL")
os.makedirs('eda_outputs', exist_ok=True)

def plot_temp_analysis():
    engine = create_engine(DB_URL)
    
    print("🔗 Fetching enriched data for Nairobi...")
    # SQL Query: Keep column names identical to what the model expects ('y')
    query = """
    SELECT 
        ds, 
        county, 
        y, 
        surface_pressure, 
        temp_k 
    FROM kenya_enriched_data 
    WHERE county = 'Nairobi' 
    ORDER BY ds
    """
    df = pd.read_sql(query, engine)
    df['ds'] = pd.to_datetime(df['ds'])

    # 1. LOAD MODELS
    try:
        prophet_model = joblib.load('prophet_outputs/models/prophet_nairobi.joblib')
        xgb_model = joblib.load('xgb_outputs/models/kenya_xgboost_v1.joblib')
        print("✅ Models loaded successfully.")
    except Exception as e:
        print(f"❌ Error loading models: {e}")
        return

    # 2. PROPHET PREDICTIONS
    print("🔮 Generating Prophet predictions...")
    prophet_df = df[['ds']].copy()
    forecast = prophet_model.predict(prophet_df)
    df['prophet_pred'] = forecast['yhat'].values
    
    # 3. XGBOOST PREDICTIONS
    print("🛠️ Reconstructing features for XGBoost...")
    # Feature Engineering (must match training logic exactly)
    df['temp_lag_6h'] = df['y'].shift(1)
    df['temp_lag_24h'] = df['y'].shift(4)
    df['county_cat'] = 0 # Ensure this matches your training encoding for Nairobi
    
    # Handle NaNs from shifts using modern pandas syntax
    df = df.bfill()
    
    # CRITICAL: Feature names and order MUST match the training set
    # Based on your error, order is: ['y', 'surface_pressure', 'temp_k', 'temp_lag_6h', 'temp_lag_24h', 'county_cat']
    features = ['y', 'surface_pressure', 'temp_k', 'temp_lag_6h', 'temp_lag_24h', 'county_cat']
    X_xgb = df[features]
    
    try:
        df['xgb_pred'] = xgb_model.predict(X_xgb)
    except Exception as e:
        print(f"❌ XGBoost Prediction Failed: {e}")
        return

    # 4. VISUALIZATION
    plt.figure(figsize=(15, 8))
    
    # Plot Ground Truth
    plt.plot(df['ds'], df['y'], label='Actual (Ground Truth)', color='black', alpha=0.3, linewidth=1)
    
    # Plot Prophet
    plt.plot(df['ds'], df['prophet_pred'], label='Prophet (Seasonality Only)', color='green', linewidth=2)
    
    # Plot XGBoost
    plt.plot(df['ds'], df['xgb_pred'], label='XGBoost (Features + Lags)', color='blue', linestyle='--', alpha=0.8)

    plt.title('Nairobi Temperature Prediction: Model Comparison')
    plt.ylabel('Temperature (°C)')
    plt.xlabel('Timeline')
    plt.legend(loc='upper right')
    plt.grid(True, linestyle='--', alpha=0.5)

    # Zoom into the last 14 days for high-detail comparison
    if len(df) > 56: # Assuming 6-hourly data
        last_date = df['ds'].max()
        start_date = last_date - pd.Timedelta(days=14)
        plt.xlim(start_date, last_date)
        plt.title('Nairobi Comparison: Last 14 Days (Zoomed)')

    output_path = 'eda_outputs/nairobi_model_comparison_final.png'
    plt.savefig(output_path, dpi=300)
    print(f"✅ Visual successfully saved to {output_path}")
    plt.show()

if __name__ == "__main__":
    plot_temp_analysis()