import pandas as pd
import joblib
import folium
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")

def generate_local_map():
    engine = create_engine(DB_URL)
    
    print("🔗 Fetching data for spatial visualization...")
    # Get the latest snapshot for every location
    query = """
    SELECT DISTINCT ON (latitude, longitude)
        ds, latitude, longitude, county, y, surface_pressure, temp_k
    FROM kenya_enriched_data
    ORDER BY latitude, longitude, ds DESC
    """
    df = pd.read_sql(query, engine)

    # 1. LOAD MODEL & PREDICT
    try:
        xgb_model = joblib.load('xgb_outputs/models/kenya_xgboost_v1.joblib')
        
        # Prepare features (Matching your training schema)
        df['temp_lag_6h'] = df['y'] 
        df['temp_lag_24h'] = df['y']
        df['county_cat'] = 0 
        
        features = ['y', 'surface_pressure', 'temp_k', 'temp_lag_6h', 'temp_lag_24h', 'county_cat']
        df['risk_score'] = xgb_model.predict(df[features])
        print("✅ Risk scores calculated.")
    except Exception as e:
        print(f"⚠️ Prediction failed, using raw temperature for map. Error: {e}")
        df['risk_score'] = df['y']

    # 2. CREATE FOLIUM MAP
    # Center map on Kenya
    m = folium.Map(location=[0.0236, 37.9062], zoom_start=6, tiles='CartoDB positron')

    # 3. ADD POINTS TO MAP
    for _, row in df.iterrows():
        # Color logic: Red if temp/risk is high, Blue if low
        color = 'red' if row['risk_score'] > 25 else 'blue'
        
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=8,
            popup=(f"<b>County:</b> {row['county']}<br>"
                   f"<b>Current Temp:</b> {row['y']:.2f}°C<br>"
                   f"<b>ML Risk Score:</b> {row['risk_score']:.2f}"),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7
        ).add_to(m)

    # 4. SAVE MAP
    output_path = 'kenya_climate_risk_map.html'
    m.save(output_path)
    print(f"✅ SUCCESS: Map generated! Open '{output_path}' in your browser to view it.")

if __name__ == "__main__":
    generate_local_map()