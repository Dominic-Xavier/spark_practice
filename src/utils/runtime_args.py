import sys

def get_env_arg():
    """
    Get --env argument in a way that works for:
    - Local (python main.py --env dev)
    - EMR (spark-submit main.py --env dev)
    - AWS Glue (job parameter --env=prod)
    """

    # ---------- AWS GLUE ----------
    try:
        from awsglue.utils import getResolvedOptions

        # If this import works, we ARE in Glue
        args = getResolvedOptions(sys.argv, ["env"])
        return args["env"]

    except ImportError:
        # ---------- LOCAL / EMR ----------
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--env", required=True)

        # IMPORTANT: parse_known_args, not parse_args
        args, _ = parser.parse_known_args()
        return args.env

