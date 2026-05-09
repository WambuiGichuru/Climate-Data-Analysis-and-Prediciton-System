import pandas as pd
from xgboost import XGBRegressor
from google.cloud import bigquery
from sklearn.model_selection import train_test_split
import joblib

# Logic: Fetching from the BigQuery tables built in Milestone 2/3
client = bigquery.Client()
query = """
    SELECT * FROM `sds2412-kenya-onset.ml_features.nairobi_daily_weather`
    WHERE year BETWEEN 1990 AND 2023
"""
df = client.query(query).to_dataframe()

# Feature Engineering logic (6h and 24h lags)
df['temp_lag_6h'] = df['temperature'].shift(1)
df['temp_lag_24h'] = df['temperature'].shift(4)
df.dropna(inplace=True)

# Train/Test Split logic for operationalization
X = df[['temp_lag_6h', 'temp_lag_24h', 'precipitation']]
y = df['target_temperature']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

# Model initialization with high-scale parameters
model = XGBRegressor(n_estimators=1000, learning_rate=0.05, max_depth=6)
model.fit(X_train, y_train)

# Save the artifact for Vertex AI Deployment (Milestone 4 requirement)
joblib.dump(model, "nairobi_xgboost_v1.joblib")
print("✅ Model trained and serialized for Cloud Deployment.")