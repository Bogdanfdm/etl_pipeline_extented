import pandas as pd
import os 
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
import duckdb as ddb
from pathlib import Path
import logging 

logger = logging.getLogger(__name__)

path_csv = Path(__file__).resolve().parent.parent / 'data' / 'data_for_insert' / 'csv'

ddb_con = ddb.connect()

def parse_csv_to_dict(csv_path: Path,
                      dict_tables_names_data: dict) -> dict:
    # parsing csv files to dfs using duckdb
    for csv in path_csv.glob('*.csv'):
        print

for csv in path_csv.glob("*.csv"):
    print(f"reading {csv}")
    df = ddb_con.execute(
        f"""
        SELECT * FROM read_csv_auto('{csv}') LIMIT 100
         """
    ).df()
    print(df)

# записать каждый датафрейм в словарь, в котором будет ключ значение имя таблицы - сам датафрейм