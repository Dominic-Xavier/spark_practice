import os
import json
from utils.path_utils import resolve_path

class WaterMarkManager:

    def __init__(self, watermark_file_path: str, logger=None):
        self.watermark_file_path = watermark_file_path
        self.logger = logger

    def _create_file_if_not_exists(self):
        """
        Creates watermark file if it does not exist and directory exists
        """

        file_path = self.watermark_file_path

        directory = os.path.dirname(file_path)

        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        
        if not os.path.exists(file_path):
            with open(file_path, "w") as f:
                f.write("")


    def read_watermark(self, key: str):
        """
        Reads watermark value for a given key
        """
        self._create_file_if_not_exists()

        if os.path.exists(self.watermark_file_path) and os.path.getsize(self.watermark_file_path) == 0:
            self.logger.info(f"Watermark file {self.watermark_file_path} is empty.")
            return None


        with open(self.watermark_file_path, "r") as f:
            data = json.load(f)
        
        if data is None:
            return data
        
        value = data.get(key)
        return value

    def update_watermark(self, **kwargs):
        """
        Updates watermark value for a given key
        """

        self._create_file_if_not_exists()

        with open(self.watermark_file_path, "r") as f:
            existing_data = json.load(f)

        existing_data.update(kwargs)

        with open(self.watermark_file_path, "w") as f:
            json.dump(existing_data, f, indent=2)