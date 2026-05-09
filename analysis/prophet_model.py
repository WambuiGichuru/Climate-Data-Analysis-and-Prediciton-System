# prophet_final_render.py
import pandas as pd
from prophet import Prophet
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# 1. Database Connection & Data Load
engine = create_engine("postgresql://root:root@ingest-pgdatabase-1:5432/cds_data")
df = pd.read_sql("SELECT valid_time as ds, temperature_c as y FROM kenya_temporal_data", engine)

# 2. Model Training
model = Prophet(daily_seasonality=True, yearly_seasonality=True)
model.fit(df)

# 3. Forecast Generation
future = model.make_future_dataframe(periods=14*4, freq='6h')
forecast = model.predict(future)

# 4. Prepare Plots (Queue them up without showing yet)
fig1 = model.plot(forecast)
plt.title("Nairobi Temperature: Main Forecast")

fig2 = model.plot_components(forecast)
# No title needed here as Prophet titles individual subplots

# 5. The "Magic" Command
# This will open both windows simultaneously (or in sequence depending on your OS)
# but it ensures the script doesn't hang between them.
plt.show()