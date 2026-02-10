from pyspark.sql import functions as F
from functools import reduce


def multi_join(base_df, join_configs):
    return reduce(lambda df, cfg: df.join(
        F.broadcast(cfg["df"]) if cfg.get("broadcast", False) else cfg["df"], 
                    cfg["on"], cfg["how"]),
                    join_configs, base_df)