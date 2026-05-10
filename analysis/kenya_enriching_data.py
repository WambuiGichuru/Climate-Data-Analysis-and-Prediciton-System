import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. SETUP & CREDENTIALS
load_dotenv()
DB_URL = os.getenv("DB_URL")

def run_spatial_enrichment():
    if not DB_URL:
        print("❌ Error: DB_URL not found in .env file.")
        return

    engine = create_engine(DB_URL)
    
    # 2. EXTRACT UNIQUE COORDINATES
    print("🔗 Connecting to PostgreSQL...")
    try:
        coord_query = "SELECT DISTINCT latitude, longitude FROM kenya_temporal_data"
        coords = pd.read_sql(coord_query, engine)
        print(f"📍 Found {len(coords)} unique coordinate point(s) in the dataset.")
    except Exception as e:
        print(f"❌ Error reading coordinates: {e}")
        return

    # 3. SPATIAL MAPPING LOGIC
    # Maps the coordinate point found in your table to 'Nairobi_Region'
    def get_county_name(lat, lon):
        if lat == 1.25 and lon == 36.75:
            return "Nairobi"
        elif -4.2 <= lat <= -3.8 and 39.4 <= lon <= 39.8:
            return "Mombasa"
        elif 3.0 <= lat <= 4.0 and 35.0 <= lon <= 36.5:
            return "Turkana"
        else:
            return f"Region_{lat}_{lon}"

    print("🗺️ Mapping coordinates to administrative names...")
    coords['county'] = coords.apply(lambda x: get_county_name(x['latitude'], x['longitude']), axis=1)

    # 4. CREATE ENRICHED TABLE IN POSTGRESQL
    print("🔄 Processing database enrichment...")
    try:
        with engine.connect() as conn:
            # Upload the temporary mapping dataframe
            coords.to_sql('coord_mapping', engine, if_exists='replace', index=False)
            
            # Drop existing table if it exists to ensure a fresh start
            conn.execute(text("DROP TABLE IF EXISTS kenya_enriched_data"))
            
            # Create the final enriched table using text() for SQLAlchemy 2.0+
            enrichment_sql = text("""
                CREATE TABLE kenya_enriched_data AS
                SELECT 
                    t.valid_time as ds,
                    m.county,
                    t.latitude,
                    t.longitude,
                    t.temperature_c as y,
                    t.sp as surface_pressure,
                    t.t2m as temp_k
                FROM kenya_temporal_data t
                JOIN coord_mapping m ON t.latitude = m.latitude AND t.longitude = m.longitude
            """)
            
            conn.execute(enrichment_sql)
            conn.commit()  # Explicitly commit the transaction
            
        print("✅ SUCCESS: 'kenya_enriched_data' table created.")
        print("📊 Ready for modeling with the 'county' column!")

    except Exception as e:
        print(f"❌ Error during SQL execution: {e}")

if __name__ == "__main__":
    run_spatial_enrichment()