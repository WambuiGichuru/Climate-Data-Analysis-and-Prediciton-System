import cdsapi
import xarray as xr
import pandas as pd
import os
import click
from sqlalchemy import create_engine


def ingest_data(
    dataset: str,
    variables: list,
    year: str,
    month: str,
    day: str,
    time: str,
    pressure_level: str,
    download_dir: str,
    db_user: str,
    db_password: str,
    db_host: str,
    db_port: str,
    db_name: str,
    table_name: str = None,
    chunksize: int = 5000
):
    client = cdsapi.Client()

    request = {
        'product_type': ['reanalysis'],
        'variable': variables,
        'year': [year],
        'month': [month],
        'day': [day],
        'time': [time],
        'pressure_level': [pressure_level],
        'data_format': 'grib',
    }

    os.makedirs(download_dir, exist_ok=True)

    file_name = f"{dataset}_{year}_{month}_{day}.grib"
    target = os.path.join(download_dir, file_name)

    print("Downloading data from CDS...")
    client.retrieve(dataset, request, target)

    print("Opening GRIB dataset...")
    ds = xr.open_dataset(
        target,
        engine='cfgrib',
        backend_kwargs={'filter_by_keys': {'shortName': 't'}}
    )

    print("Transforming data...")
    df = ds.to_dataframe().reset_index()

    df = df.rename(columns={
        'latitude':      'lat',
        'longitude':     'lon',
        'valid_time':    'valid_time',
        'isobaricInhPa': 'pressure_hpa',
        't':             'temperature_k',
    })

    if 'temperature_k' in df.columns:
        df['temperature_c'] = df['temperature_k'] - 273.15

    df = df.dropna(subset=['temperature_k'])

    if 'step' in df.columns:
        df['step_hours'] = df['step'].dt.total_seconds().div(3600).astype(int)
        df = df.drop(columns=['step'])

    print(f"Final shape: {df.shape}")

    db_uri = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(db_uri)

    if not table_name:
        table_name = file_name.replace('.grib', '').replace('-', '_').lower()

    print(f"Ingesting into table: {table_name}")

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=chunksize
    )

    print("Ingestion complete ✅")

    return {
        "table": table_name,
        "rows": len(df),
        "file": target
    }


@click.command()
@click.option("--dataset", required=True, type=str)
@click.option("--variables", required=True, multiple=True, type=str)
@click.option("--year", required=True, type=str)
@click.option("--month", required=True, type=str)
@click.option("--day", required=True, type=str)
@click.option("--time", "time_", required=True, type=str)
@click.option("--pressure-level", required=True, type=str)
@click.option("--download-dir", required=True, type=str)
@click.option("--db-user", required=True, type=str)
@click.option("--db-password", required=True, type=str)
@click.option("--db-host", required=True, type=str)
@click.option("--db-port", required=True, type=str)
@click.option("--db-name", required=True, type=str)
@click.option("--table-name", default=None, type=str)
@click.option("--chunksize", default=5000, show_default=True, type=int)
def cli(
    dataset: str,
    variables: tuple,
    year: str,
    month: str,
    day: str,
    time_: str,
    pressure_level: str,
    download_dir: str,
    db_user: str,
    db_password: str,
    db_host: str,
    db_port: str,
    db_name: str,
    table_name: str,
    chunksize: int
):
    ingest_data(
        dataset=dataset,
        variables=list(variables),
        year=year,
        month=month,
        day=day,
        time=time_,
        pressure_level=pressure_level,
        download_dir=download_dir,
        db_user=db_user,
        db_password=db_password,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        table_name=table_name,
        chunksize=chunksize,
    )


if __name__ == "__main__":
    cli()