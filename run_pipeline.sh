#!/bin/bash
set -e

echo "Running full batch pipeline..."

python src/ingest/noaa_ghcnd_ingest.py --test || exit 1
echo "NOAA ingest done."

python src/ingest/openmeteo_historical_ingest.py || exit 1
echo "OpenMeteo ingest done."

python src/batch/noaa_spark_processor.py || exit 1
echo "Spark batch processing done."

python src/ml/feature_engineer.py || exit 1
echo "Feature engineering done."

python src/ml/model_trainer.py || exit 1
echo "Model training done."

echo ""
echo "Pipeline complete. Models saved to models/"
