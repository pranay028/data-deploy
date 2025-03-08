import json
import boto3
import requests
import os
from dotenv import load_dotenv
from ..utils.s3_helper import upload_to_s3
from ..utils.s3_config import RAW_FOLDER, PROCESSED_FOLDER
from ..utils.log_config import logger
from ..utils.s3_helper import s3_client,BUCKET_NAME


load_dotenv()
API_KEY = os.getenv("GITHUB_API_KEY")

def transform_raw_data():
    """
    Returns a dict that contains username, user_id, github_url.
    """
    prefix = f"{RAW_FOLDER}/github_users_raw"
    
    try:
        logger.info("Fetching raw data from S3...")
        get_all_files = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        
        if "Contents" not in get_all_files:
            logger.warning("No files exist in the bucket: %s", BUCKET_NAME)
            return None
        
        sorted_files = sorted(get_all_files["Contents"], key=lambda x: x['LastModified'], reverse=True)
        file_path = sorted_files[0]["Key"]

        logger.info("Fetching latest file: %s", file_path)
        data = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_path)

    except Exception as e:
        logger.error("Error occurred while fetching data from S3 bucket %s: %s", BUCKET_NAME, str(e))
        raise ValueError(f"S3 access failed for bucket {BUCKET_NAME}")

    data_read = json.loads(data["Body"].read().decode("utf-8"))

    if not isinstance(data_read, list):
        logger.error("Expected list of user data but received a different type")
        raise ValueError("Error: Expected list of user data but received another type")

    total_users = []
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {API_KEY}"
    }

    for user in data_read:
        try:
            response = requests.get(user["url"], headers=headers)
            user_data = response.json()

            total_users.append({
                "username": user.get("login"),
                "name": user_data.get("name", ""),
                "company": user_data.get("company", "N/A"),
                "location": user_data.get("location", "N/A"),
                "public_repos": user_data.get("public_repos", 0),
                "github_url": user.get("html_url"),
                "user_api_url": user.get("url"),
                "followers_count": user_data.get("followers", 0),
            })

        except Exception as e:
            logger.warning("Error occurred while fetching user(%s) info: %s", user.get("login"), str(e))

    return total_users


def upload_transform_data():
    """
    Transforms raw data and uploads it to S3.
    """
    data = transform_raw_data()

    if data is not None:
        json_data = json.dumps(data)
        transformed_path = f"{PROCESSED_FOLDER}/github_users_processed.json"
        logger.info("Uploading transformed data to S3: %s", transformed_path)

        upload_to_s3(json_data, transformed_path)
        logger.info("Transformed data uploaded successfully!")

    else:
        logger.error("No transformed data available to upload")
        raise ValueError("Bucket has no such folders")



if __name__ == "__main__":
    upload_transform_data()
