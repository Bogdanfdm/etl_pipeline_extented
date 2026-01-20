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
logger.debug("path to csv files: %s", path_csv)
ddb_con = ddb.connect()

df_dicts = {}

def parse_csv_to_dict(csv_path: Path,
                      dict_tables_names_data: dict) -> dict:
    # parsing csv files to dfs using duckdb
    for csv in csv_path.glob('*.csv'):
        # logger.debug("working with %s", csv)
        
        df =ddb_con.execute(
            f"""
            select * from read_csv_auto('{csv}') limit 5000
            """
        ).df()
        df_name = csv.stem
        # logger.debug("name of df: %s", df_name)
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
            logger.debug("written to parquet for %s", df_name)
        logger.info("all dfs written to parquet on %s", output_path)

        return None

path_parquet = Path(__file__).resolve().parent.parent / 'data' / 'pq_files'

df_to_parquet(dfs_dict = parse_csv_to_dict(path_csv, df_dicts), 
              output_path=path_parquet)

logger.info("its stil work!")


staging_schema_name = "staging"

sql_schema_creation = f"""
create schema if not exists {staging_schema_name};
"""

logger.info("sql statement looks like: %s", sql_schema_creation)

# Add a staging layer to postgres for csv data 

engine = create_engine(
     f"postgresql+psycopg2://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_HOST}:{settings.POSTGRES_OUTSIDE_PORT}/{settings.POSTGRES_DB}"
)
logger.info("trying to run %s sql script", sql_schema_creation)

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
        # logger.debug("DDL:\n%s", sql_statement_create_table)

        with engine.begin() as con:
             con.execute(text(sql_statement_create_table))

staging_schema_tables_creation()

logger.info("Created staging schema in postgres: %s", staging_schema_name)

