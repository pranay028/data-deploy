import os
from datetime import datetime
from .s3_config import LOGS_FOLDER
from .logs import logger, LOCAL_LOG_PATH, LOG_FILE
from .s3_helper import BUCKET_NAME, s3_client


S3_LOG_FILE_PATH =f"{LOGS_FOLDER}/{LOG_FILE}"


def upload_log_to_s3():
    
    try:
        s3_client.upload_file(LOCAL_LOG_PATH, BUCKET_NAME, S3_LOG_FILE_PATH)
        logger.info(f"Log file uploaded to S3: s3://{BUCKET_NAME}/{S3_LOG_FILE_PATH}")
    except Exception as e:
        logger.error(f"Failed to upload log file to S3: {str(e)}")
        