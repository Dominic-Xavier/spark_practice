"""
Batch job: consolidate fragmented Delta files every 30 minutes.
Merges small parquet files into fewer, larger files to improve query performance.
"""
from pyspark.sql import SparkSession
from datetime import datetime
import sys

def consolidate_delta_table(table_path, target_partitions=4):
    """
    Consolidate small files in a Delta table by rewriting with fewer partitions.
    
    Args:
        table_path: Path to Delta table
        target_partitions: Target number of partitions after consolidation
    """
    spark = SparkSession.builder \
        .appName("DeltaConsolidation") \
        .getOrCreate()
    
    try:
        # Read Delta table
        print(f"[{datetime.now()}] Reading Delta table from {table_path}...")
        df = spark.read.format("delta").load(table_path)
        
        row_count = df.count()
        print(f"[{datetime.now()}] Total rows: {row_count}")
        
        if row_count == 0:
            print(f"[{datetime.now()}] No data to consolidate. Exiting.")
            return
        
        # Repartition and write back with mode 'overwrite'
        print(f"[{datetime.now()}] Consolidating into {target_partitions} partitions...")
        df.coalesce(target_partitions) \
            .write \
            .mode("overwrite") \
            .format("delta") \
            .save(table_path)
        
        print(f"[{datetime.now()}] ✓ Consolidation complete!")
        
        # Optimize the table (if supported by Delta)
        try:
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
            print(f"[{datetime.now()}] ✓ Table optimized!")
        except Exception as e:
            print(f"[{datetime.now()}] OPTIMIZE not available: {e}")
    
    except Exception as e:
        print(f"[{datetime.now()}] ✗ Error during consolidation: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    # Path to the Delta table to consolidate
    delta_table_path = "/home/dominic/Desktop/spark_practice/output/olist_orders"
    
    # Run consolidation with 4 target partitions
    consolidate_delta_table(delta_table_path, target_partitions=4)
