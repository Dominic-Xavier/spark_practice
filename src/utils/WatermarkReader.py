import json
import boto3
from botocore.exceptions import ClientError

class WatermarkReader:

    def __init__(self, s3_path):
        """
        s3_path example:
        s3://my-bucket/watermarks/watermark.json
        """
        self.s3_path = s3_path
        self.s3 = boto3.client("s3")

    def _parse_s3_path(self):
        if not self.s3_path.startswith("s3://"):
            raise ValueError("Invalid S3 path")

        path = self.s3_path.replace("s3://", "")
        bucket, key = path.split("/", 1)
        return bucket, key

    def _create_empty_file_if_not_exists(self):
        """
        Creates an empty JSON file {} if file does not exist
        """
        bucket, object_key = self._parse_s3_path()

        try:
            self.s3.head_object(Bucket=bucket, Key=object_key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.s3.put_object(
                    Bucket=bucket,
                    Key=object_key,
                    Body=json.dumps({}),
                    ContentType="application/json"
                )
            else:
                raise

    def read_watermark(self, key):
        bucket, object_key = self._parse_s3_path()

        self._create_empty_file_if_not_exists()

        try:
            response = self.s3.get_object(Bucket=bucket, Key=object_key)
            content = response["Body"].read().decode("utf-8").strip()

            if not content or content == "{}":
                return None

            data = json.loads(content)

            if key not in data:
                raise KeyError(f"Invalid watermark key: {key}")

            return data[key]

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            else:
                raise

    def update_watermark(self, **data):
        """
        Updates one or more watermark keys.

        Example:
        update_watermark(
            sales_last_processed_ts="2025-01-21T00:00:00",
            customer_last_id=12345
        )
        """

        if not data:
            raise ValueError("No watermark data provided")

        # Ensure file exists
        self._create_empty_file_if_not_exists()

        bucket, object_key = self._parse_s3_path()

        response = self.s3.get_object(Bucket=bucket, Key=object_key)
        content = response["Body"].read().decode("utf-8").strip()
        existing_data = json.loads(content) if content else {}

        # Validate keys if file already had data
        if existing_data:
            invalid_keys = set(data.keys()) - set(existing_data.keys())
            if invalid_keys:
                raise KeyError(f"Invalid watermark keys: {invalid_keys}")

        # Merge updates
        existing_data.update(data)

        # Atomic overwrite
        self.s3.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=json.dumps(existing_data, indent=2),
            ContentType="application/json"
        )
