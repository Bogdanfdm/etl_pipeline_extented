import pandas as pd
import os 
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
import duckdb as ddb
from pathlib import Path
import logging 

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
    for csv in path_csv.glob('*.csv'):
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

# записать каждый датафрейм в словарь, в котором будет ключ значение имя таблицы - сам датафрейм