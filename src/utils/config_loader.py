import yaml
import os
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

    file_path = "s3://employee-records-1001/Git-Code/config/"
    full_path = f"{file_path}/{env}.yaml"
    if env == "prod":
        with open(full_path, "r") as file:
            config = yaml.safe_load(file)
    elif env == "dev":
        with open(get_project_root() + f"\\config\\{env}.yaml", "r") as file:
            config = yaml.safe_load(file)

    return config
