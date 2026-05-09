import pandas as pd
import xgboost as xgb
from prophet import Prophet
from prophet.serialize import model_to_json
from sqlalchemy import create_engine
import joblib # Excellent for saving XGBoost
import json

# 1. Load Data
engine = create_engine("postgresql://root:root@ingest-pgdatabase-1:5432/cds_data")
df = pd.read_sql("SELECT valid_time, temperature_c FROM kenya_temporal_data ORDER BY valid_time", engine)

# --- PART A: PROPHET ---
print("Saving Prophet...")
prophet_df = df.rename(columns={'valid_time': 'ds', 'temperature_c': 'y'})
model_p = Prophet(daily_seasonality=True)
model_p.fit(prophet_df)

with open('nairobi_prophet_model.json', 'w') as fout:
    fout.write(model_to_json(model_p))

# --- PART B: XGBOOST ---
print("Saving XGBoost...")
# Re-apply our feature engineering
df['valid_time'] = pd.to_datetime(df['valid_time'])
df.set_index('valid_time', inplace=True)
df['hour'] = df.index.hour
df['month'] = df.index.month
df['lag_6h'] = df['temperature_c'].shift(1)
df['lag_24h'] = df['temperature_c'].shift(4)
df_xgb = df.dropna()

features = ['hour', 'month', 'lag_6h', 'lag_24h']
model_x = xgb.XGBRegressor(n_estimators=100)
model_x.fit(df_xgb[features], df_xgb['temperature_c'])

# Save using Joblib (cleaner than pickle for XGBoost)
joblib.dump(model_x, 'nairobi_xgboost_model.joblib')

print("🎉 Both models saved to disk: 'nairobi_prophet_model.json' and 'nairobi_xgboost_model.joblib'")