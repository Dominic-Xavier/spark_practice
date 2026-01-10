import os

def get_project_root():
    """
    Returns absolute path of project root directory
    """
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )

def resolve_path(relative_path: str):
    return get_project_root() + relative_path

str = get_project_root() + "\\config\\dev.yaml"

print(str)