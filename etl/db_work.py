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
    # parsing csv files to dfs using duckdb
    for csv in csv_path.glob('*.csv'):
        logger.debug("working with %s", csv)
        
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

logger.info("done this %s", df_dicts_full['common_player_info'])

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


schema_name = "nba_data"

sql_schema_creation = f"""
create schema if not exists {schema_name} authorization user;
"""

logger.info("sql statement looks like: %s", sql_schema_creation)

# Сейчас нужно разобрать модель данных, найти фк и пк, создать ддл и записать таблицы в бд

# sql_table_creation = """
# create table if not exists 
# """

# def create_table_sql(
#           conn_str: CONN_DB_SETTINGS,
#           sql_statement: str,
#           table_dict: dict
# ):
#     conn = psycopg2.connect(
#           database=conn_str.POSTGRES_DB,
#           user = conn_str.POSTGRES_USER,
#           password = conn_str.POSTGRES_PASSWORD,
#           host=conn_str.POSTGRES_HOST,
#           port = str(conn_str.POSTGRES_OUTSIDE_PORT)
#      )
#     conn.autocommit = True
#     cursor = conn.cursor()
#     for table_name, df in table_dict.itmes():
         