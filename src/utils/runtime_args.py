import sys


def get_env_arg():
    """
    Returns env argument in both local and Glue environments
    """

    # Detect Glue runtime
    if "awsglue" in sys.modules:
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv, ["env"])
        return args["env"]

    # Local / EMR runtime
    else:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--env", required=True)
        args, _ = parser.parse_known_args()
        return args.env
