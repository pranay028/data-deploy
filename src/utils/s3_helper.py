import json
import boto3
import os
from dotenv import load_dotenv
from .logs import logger


load_dotenv()

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
REGION_NAME = os.environ.get("AWS_REGION")



s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)

def upload_to_s3(data, file_path):
    """
    Uploads API data to S3 bucket
    :param data: JSON response from source API (should be a dictionary or a JSON string)
    :param key: S3 key (file path)
    :param bucket: S3 bucket name
    """
    try:
        if isinstance(data, dict):  
            data = json.dumps(data)  

        elif not isinstance(data, str):
            logger.error("Unsupported Data Format, expected JSON string or dictionary")
            raise ValueError("Unsupported Data Format, expected JSON string or dictionary")


        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_path,
            Body=data,
            ContentType='application/json'
        )
        logger.info(f"uploaded to S3 bucket {file_path}")

    except Exception as e:
        logger.error(f"Error Uploading to S3:", e)
