import os
import json
from botocore.exceptions import ClientError
from json import JSONDecodeError

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
                json.dump({}, f)


    def read_watermark(self, key):
        bucket, object_key = self._parse_s3_path()

        # Ensure file exists
        self._create_empty_file_if_not_exists()

        try:
            response = self.s3.get_object(Bucket=bucket, Key=object_key)
            content = response["Body"].read().decode("utf-8").strip()

            # Empty or whitespace-only file
            if not content:
                return None

            try:
                data = json.loads(content)
            except JSONDecodeError:
                # Corrupt / partial / invalid JSON
                self.s3.put_object(
                    Bucket=bucket,
                    Key=object_key,
                    Body=json.dumps({}),
                    ContentType="application/json"
                )
                return None

            if not data:
                return None

            if key not in data:
                raise KeyError(f"Invalid watermark key: {key}")

            return data[key]

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            else:
                raise

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