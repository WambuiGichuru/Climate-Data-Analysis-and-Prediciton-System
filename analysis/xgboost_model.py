# xgboost_prediction.py
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# 1. Load and Feature Engineer
engine = create_engine("postgresql://root:root@ingest-pgdatabase-1:5432/cds_data")
df = pd.read_sql("SELECT valid_time, temperature_c FROM kenya_temporal_data ORDER BY valid_time", engine)

df['valid_time'] = pd.to_datetime(df['valid_time'])
df.set_index('valid_time', inplace=True)
df['hour'] = df.index.hour
df['month'] = df.index.month
df['lag_6h'] = df['temperature_c'].shift(1)
df['lag_24h'] = df['temperature_c'].shift(4)
df = df.dropna()

# 2. Train (80/20 split)
features = ['hour', 'month', 'lag_6h', 'lag_24h']
X, y = df[features], df['temperature_c']
split = int(len(X) * 0.8)
X_train, X_test, y_train, y_test = X[:split], X[split:], y[:split], y[split:]

model = xgb.XGBRegressor(n_estimators=100, learning_rate=0.1)
model.fit(X_train, y_train)

# 3. Visualize
preds = model.predict(X_test)
plt.figure(figsize=(12,6))
plt.plot(y_test.index, y_test, label='Actual', alpha=0.5)
plt.plot(y_test.index, preds, color='red', linestyle='--', label='XGBoost')
plt.title(f"XGBoost Performance (RMSE: {np.sqrt(((preds - y_test) ** 2).mean()):.2f}°C)")
plt.legend()
plt.show()