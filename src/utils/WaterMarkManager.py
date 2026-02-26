import os
import json
from json import JSONDecodeError
from src.utils.WaterMark import WaterMark

class WaterMarkManager(WaterMark):

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
                json.dump({}, f)


    def read_watermark(self, key: str):
        """
        Read value from JSON content.

        Args:
            json_content (str): JSON string
            key (str): key to read

        Returns:
            value | None

        Raises:
            KeyError: if key is not present
            ValueError: if JSON is invalid
        """

        self._create_file_if_not_exists()

        # Handle empty / whitespace-only data
        

        try:
            with open(self.watermark_file_path, "r") as f:
                data = json.load(f)
        except JSONDecodeError:
            return None

        # Handle empty JSON object
        if not data or data == {} or key not in data:
            return None

        return data[key]

    def update_watermark(self, **kwargs):
        """
        Updates watermark value for a given key
        """

        self._create_file_if_not_exists()

        with open(self.watermark_file_path, "r") as f:
            existing_data = json.load(f)

        # Convert date objects to strings for JSON serialization
        for key, value in kwargs.items():
            if hasattr(value, 'isoformat'):  # Check if it's a date/datetime
                kwargs[key] = value.isoformat()

        existing_data.update(kwargs)

        with open(self.watermark_file_path, "w") as f:
            json.dump(existing_data, f, indent=2)