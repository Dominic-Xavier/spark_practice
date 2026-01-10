import argparse

def parse_args():
    """
    Parses command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Spark ETL Pipeline"
    )

    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "qa", "prod"],
        help="Environment to run the pipeline"
    )

    return parser.parse_args()
