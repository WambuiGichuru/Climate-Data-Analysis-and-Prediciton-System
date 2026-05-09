import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import joblib
from prophet import Prophet
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

# 1. SETUP & DB CONNECTION
load_dotenv()
DB_URL = os.getenv("DB_URL")

os.makedirs('prophet_outputs/models', exist_ok=True)
os.makedirs('prophet_outputs/plots', exist_ok=True)

def run_prophet_pipeline():
    engine = create_engine(DB_URL)
    
    # We pull from the ENRICHED table we just created
    print("🔗 Pulling data from 'kenya_enriched_data'...")
    query = "SELECT ds, county, y FROM kenya_enriched_data"
    df = pd.read_sql(query, engine)
    df['ds'] = pd.to_datetime(df['ds'])
    
    all_metrics = []
    all_residuals = []
    counties = df['county'].unique()
    
    print(f"🚀 Found {len(counties)} region(s): {counties}. Starting Prophet training...")

    for county in counties:
        print(f"📈 Modeling: {county}...")
        
        # Prepare data
        county_data = df[df['county'] == county][['ds', 'y']].sort_values('ds')
        
        # Operational Split (e.g., Use the last 20% for testing if needed)
        # For a full production run, we train on all available data before saving
        train_data = county_data.copy()
        
        # Train Prophet
        # yearly_seasonality=True is critical for Kenya's bimodal rain patterns
        m = Prophet(
            yearly_seasonality=True, 
            weekly_seasonality=False, 
            daily_seasonality=False,
            interval_width=0.95 # 95% confidence intervals for risk assessment
        )
        m.fit(train_data)
        
        # 2. FORECASTING & EVALUATION
        # Create 12 months of future "risk window"
        future = m.make_future_dataframe(periods=12, freq='MS')
        forecast = m.predict(future)
        
        # Calculate in-sample residuals for the Error Distribution plot
        # This shows how well the model "understood" the history
        performance_check = forecast.set_index('ds')[['yhat']].join(train_data.set_index('ds')).dropna()
        resids = performance_check['y'] - performance_check['yhat']
        all_residuals.extend(resids.tolist())
        
        mae = mean_absolute_error(performance_check['y'], performance_check['yhat'])
        all_metrics.append({'county': county, 'mae': mae})

        # 3. GENERATE THE VISUALS FOR MILESTONE 4
        # A. Forecast Plot (The "Projected Temperatures" view)
        fig1 = m.plot(forecast)
        plt.title(f'Prophet Onset Prediction Baseline: {county}')
        plt.xlabel('Date')
        plt.ylabel('Temperature / Rainfall Metric')
        plt.savefig(f'prophet_outputs/plots/{county}_forecast.png')
        plt.close()
        
        # B. Components Plot (Seasonality Analysis)
        # This is your "Interpretability" deliverable
        fig2 = m.plot_components(forecast)
        plt.savefig(f'prophet_outputs/plots/{county}_components.png')
        plt.close()

        # 4. SERIALIZE & STORE (R-04 Deliverable)
        model_filename = f'prophet_outputs/models/prophet_{county.lower()}.joblib'
        joblib.dump(m, model_filename)
        print(f"  ✅ Saved model to {model_filename}")

    # --- FINAL AGGREGATE PLOT ---
    # Global Residual Distribution Image
    plt.figure(figsize=(10, 6))
    sns.histplot(all_residuals, kde=True, color='teal', bins=30)
    plt.title('Prophet Model Error Distribution (Kenya-Wide)')
    plt.xlabel('Residual (Actual - Predicted)')
    plt.savefig('prophet_outputs/plots/global_residual_distribution.png')
    plt.close()
    
    print("\n--- Summary ---")
    print(pd.DataFrame(all_metrics))
    print("✅ Prophet Modelling Complete. Check the 'prophet_outputs' folder.")

if __name__ == "__main__":
    run_prophet_pipeline()