# Climate-Data-Analysis-and-Prediction-System

# Kenya County-Level Rainfall Onset Advisory Dashboard
## SDS 2412 — Analysis of Large Datasets
## GROUP TWO

### Project Overview
The Kenya Seasonal Rainfall Onset Advisory Dashboard is a cloud‑native, large‑scale data system that ingests 60+ years of historical climate observations and real‑time forecast streams to deliver county‑level planting advisories for rain‑fed agriculture. Built on Google Cloud Platform using a Lambda Architecture (Apache Spark batch processing and OpenMeteo/Pub/Sub streaming), the system applies machine learning to generate daily onset probability scores. The final output is an interactive web dashboard that visualizes county risk maps and historical onset trends, enabling agricultural officers and farmers to make timely, climate‑smart planting decisions.

### Team
| Role | Name | Responsibility |
|------|------|----------------|
| R01  | Dennis Gitau | Data & Infrastructure Lead [RO1] |
| R02  | Ashley Otieno | Distributed Processing Engineer [R02] |
| R03  | Alexander Kihoi | Streaming & Real-Time Engineer [R03] |
| R04  | Eric Mugo | ML & Analytics Engineer [R04] |
| R05  | Faith Gichuru | DevOps, Deployment & Reporting Lead [R05] |


## USE BRANCHES TO ENSURE WE DO NOT OVERRIDE/OVERWRITE EACH OTHERS WORK for this first milestone
main branch - Role5 only (Faith) 

feature/r01-ingest - Role1 only (Gitau) 

feature/r02-scalability -Role2 only (Ash) 

feature/r03-openmeteo -Role 3 only (Alex)
feature/r05-setup -Role5 (Faith)

## Reproducible Setup Guide (Data Scientist Handoff)

Use this section to replicate the project environment on a new Linux machine.

### 1. Clone the project

```bash
git clone <repo-url>
cd Climate-Data-Analysis-and-Prediciton-System
```

### 2. Install required tools

Install the following before running the pipeline:

1. Python 3.13+
2. Docker + Docker Compose plugin
3. Git
4. (Optional) Jupyter support in your IDE

Quick checks:

```bash
python3 --version
docker --version
docker compose version
```

### 3. Configure Copernicus CDS credentials

Create a CDS API credentials file in your home directory:

```bash
cat > ~/.cdsapirc << 'EOF'
url: https://cds.climate.copernicus.eu/api
key: <PERSONAL-ACCESS-TOKEN>
EOF
chmod 600 ~/.cdsapirc
```

Before the first download, log in to Copernicus CDS and accept the Terms of Use for the dataset:

- `reanalysis-era5-pressure-levels`

### 4. Start PostgreSQL + pgAdmin

The ingestion stack ships with a local Postgres service and pgAdmin:

```bash
cd src/ingest
docker compose up -d
docker compose ps
```

Default service credentials from `src/ingest/docker-compose.yml`:

- Postgres host: `pgdatabase` (inside Docker network) / `localhost` (from host machine)
- Postgres port: `5432`
- Postgres DB: `cds_data`
- Postgres user: `root`
- Postgres password: `root`
- pgAdmin URL: `http://localhost:8085`
- pgAdmin login: `admin@admin.com` / `root`

### 5. Build and run the ingestion container

From `src/ingest`, build the image:

```bash
docker build -t cds_ingest:v001 .
```

Run ingestion (writes data to Postgres table `era5_custom_table`):

```bash
docker run -it --rm \
	--network=ingest_default \
	-v ~/.cdsapirc:/root/.cdsapirc:ro \
	cds_ingest:v001 \
	--dataset reanalysis-era5-pressure-levels \
	--variables temperature \
	--year 2026 \
	--month 04 \
	--day 05 \
	--time 00:00 \
	--pressure-level 850 \
	--download-dir src/ingest/data/raw/copernicus \
	--db-user root \
	--db-password root \
	--db-host pgdatabase \
	--db-port 5432 \
	--db-name cds_data \
	--table-name era5_custom_table \
	--chunksize 5000
```

### 6. Verify ingestion

Check that records were inserted:

```bash
docker exec -it ingest-pgdatabase-1 psql -U root -d cds_data -c "SELECT COUNT(*) FROM era5_custom_table;"
```

### 7. Optional: local Python environment for notebooks/analysis

From project root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Launch Jupyter if needed:

```bash
jupyter notebook
```

Suggested notebooks:

- `analysis/eda_ml.ipynb`
- `src/ingest/cds_ingest.ipynb`

### 8. Shutdown services when done

```bash
cd src/ingest
docker compose down
```

To also remove volumes (deletes local Postgres data):

```bash
docker compose down -v
```
