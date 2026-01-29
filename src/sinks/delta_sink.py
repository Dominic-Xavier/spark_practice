from src.utils.path_utils import resolve_path


def write_to_delta(df, path, checkpoint, output_mode="append"):
    query =  (
        df.writeStream
        .format("delta")
        .outputMode(output_mode)
        .trigger(processingTime='10 seconds')
        .option("checkpointLocation", f"file:///{resolve_path(checkpoint)}")
        .start(f"file:///{resolve_path(path)}")
    )
    query.awaitTermination()
    return query