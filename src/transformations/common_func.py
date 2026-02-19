from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime


def multi_join(base_df, join_configs):
    return reduce(lambda df, cfg: df.join(
        F.broadcast(cfg["df"]) if cfg.get("broadcast", False) else cfg["df"], 
                    cfg["on"], cfg["how"]),
                    join_configs, base_df)

def select_columns(df:DataFrame, *col) -> DataFrame:
    return df.select(*col)

def dateTimeConvert(date_str:str, format_string):
    date_str = date_str.replace("T"," ")
    format_string = '%Y-%m-%d %H:%M:%S'
    date_object = datetime.strptime(date_str, format_string)
    return date_object