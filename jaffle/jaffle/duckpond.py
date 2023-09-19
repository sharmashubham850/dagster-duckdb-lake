from duckdb import connect
from string import Template
from sqlescapy import sqlescape
import pandas as pd


class SQL:
    def __init__(self, sql, **bindings):
        self.sql = sql
        self.bindings = bindings

class DuckDB:
    def __init__(self, options=""):
        self.options = options

    def query(self, sql: SQL):
        db = connect(':memory:')
        db.query('install httpfs;', 'load httpfs;')
        db.query(self.options)

        dataframes = collect_dataframes(sql)
        for key, value in dataframes.items():
            db.register(key, value)

        result = db.query(sql_to_string(sql))
        if result is None:
            return
        return result.df()

def sql_to_string(s: SQL):
    replacements = {}
    for key, value in s.bindings.items():
        if isinstance(value, pd.DataFrame):
            replacements[key] = f"df_{id(value)}"
        elif isinstance(value, SQL):
            replacements[key] = f"({sql_to_string(value)})"
        elif isinstance(value, str):
            replacements[key] = f"'{sqlescape(value)}'"
        elif isinstance(value, (int, float, bool)):
            replacements[key] = int(value)
        elif value is None:
            replacements[key] = "null"
        else:
            raise ValueError(f"invalid type for {key}")

        return Template(s.sql).safe_substitute(replacements)

def collect_dataframes(s: SQL):
    dataframes = {}
    for key,value in s.bindings.items():
        if isinstance(value, pd.DataFrame):
            dataframes[f"df_{id(value)}"] = value
        elif isinstance(value, SQL):
            dataframes.update(collect_dataframes(value))
        
    return dataframes


