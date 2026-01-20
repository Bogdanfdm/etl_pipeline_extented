import pandas as pd
import os 
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
import duckdb as ddb
from pathlib import Path
import logging 
import psycopg2
from pydantic_settings import BaseSettings, SettingsConfigDict
from utils.config import CONN_DB_SETTINGS, settings 


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')

handler.setFormatter(formatter)
logger.addHandler(handler)


path_csv = Path(__file__).resolve().parent.parent / 'data' / 'data_for_insert' / 'csv'
ddb_con = ddb.connect()

df_dicts = {}

def parse_csv_to_dict(csv_path: Path,
                      dict_tables_names_data: dict) -> dict:
    # parsing parts of csv files to dfs using duckdb
    for csv in csv_path.glob('*.csv'):
        
        df =ddb_con.execute(
            f"""
            select * from read_csv_auto('{csv}') limit 5000
            """
        ).df()
        df_name = csv.stem
        dict_tables_names_data[df_name] = df
    return dict_tables_names_data

df_dicts_full = parse_csv_to_dict(csv_path=path_csv, dict_tables_names_data=df_dicts)

# записать каждый датафрейм в словарь, в котором будет ключ значение имя таблицы - сам датафрейм - done


def df_to_parquet(dfs_dict: dict,
                  output_path: Path):
        for df_name, df in dfs_dict.items():
            table = pa.Table.from_pandas(df)
            output_path_file = output_path / f"{df_name}.parquet"
            pq.write_table(table, output_path_file)
        logger.info("all dfs written to parquet on %s", output_path)

        return None

path_parquet = Path(__file__).resolve().parent.parent / 'data' / 'pq_files'

df_to_parquet(dfs_dict = parse_csv_to_dict(path_csv, df_dicts), 
              output_path=path_parquet)

engine = create_engine(
     f"postgresql+psycopg2://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_HOST}:{settings.POSTGRES_OUTSIDE_PORT}/{settings.POSTGRES_DB}"
)

staging_schema_name = "staging"

sql_schema_creation = f"""
create schema if not exists {staging_schema_name};
"""

def create_staging_schema():
    with engine.begin() as con:
         con.execute(text(sql_schema_creation))
    logger.info("Created staging schema in postgres: %s", staging_schema_name)
    return None

create_staging_schema()

# Add a staging layer to postgres for csv data 


def staging_schema_tables_creation():
    for csv_name, csv in df_dicts_full.items():

        columns_sql = ",\n    ".join(
             f'"{col}" TEXT'
             for col in csv.columns
        )

        sql_statement_create_table = f"""
        create table if not exists {staging_schema_name}."{csv_name}"(
            {columns_sql}
        )
        """
        with engine.begin() as con:
             con.execute(text(sql_statement_create_table))

staging_schema_tables_creation()

logger.info("Created staging tables in postgres: %s", staging_schema_name)

def addiction_csv_files_to_staging():
    for csv_name, csv in df_dicts_full.items():
        csv_file_path = path_csv / f"{csv_name}.csv"

        with engine.begin() as con:
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                # Skip the header row
                next(f)
                con.connection.cursor().copy_expert(
                    sql=f"""
                    COPY {staging_schema_name}."{csv_name}" FROM STDIN WITH CSV
                    """,
                    file=f
                )
        logger.info("Inserted data into table: %s.%s", staging_schema_name, csv_name)

addiction_csv_files_to_staging()