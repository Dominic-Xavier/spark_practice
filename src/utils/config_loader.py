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

    with open(get_project_root() + f"\\config\\{env}.yaml", "r") as file:
        config = yaml.safe_load(file)

    return config
