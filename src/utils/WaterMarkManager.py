import os
import json
from utils.path_utils import resolve_path

class WaterMarkManager:

    def __init__(self, watermark_file_path: str):
        self.watermark_file_path = watermark_file_path

    def _create_file_if_not_exists(self):
        """
        Creates watermark file if it does not exist
        """
        directory = resolve_path(self.watermark_file_path)
        if directory and not resolve_path(directory):
            os.makedirs(directory)

        if not resolve_path(self.watermark_file_path):
            with open(self.watermark_file_path, "w") as f:
                json.dump({}, f)

    def read_watermark(self, key: str):
        """
        Reads watermark value for a given key
        """
        self._create_file_if_not_exists()

        with open(self.watermark_file_path, "r") as f:
            data = json.load(f)

        value = data.get(key)

        return value

    def update_watermark(self, **data):
        """
        Updates watermark value for a given key
        """

        self._create_file_if_not_exists()

        with open(self.watermark_file_path, "r") as f:
            data = json.load(f)

        for key, value in data.items():
             data[key] = value

        with open(self.watermark_file_path, "w") as f:
            json.dump(data, f, indent=2)