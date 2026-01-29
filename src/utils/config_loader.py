import yaml
import boto3
from utils.path_utils import get_project_root

def load_config(env: str):
    """
    Loads YAML config based on environment
    """
    """resolve_path
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "config",
        f"{env}.yaml"
    )"""

    file_path =  f"s3://employee-records-1001/Git-Code/config/{env}.yaml"
    s3_path = file_path.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)
    if env == "prod":
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        config = yaml.safe_load(response["Body"].read())
    elif env == "dev":
        with open(get_project_root() + f"/config/{env}.yaml", "r") as file:
            config = yaml.safe_load(file)

    return config
