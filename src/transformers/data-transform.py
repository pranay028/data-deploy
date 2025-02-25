import json
import boto3
from ..utils.s3_helper import upload_to_s3
from ..utils.s3_config import RAW_FOLDER, PROCESSED_FOLDER , BUCKET_NAME
import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GITHUB_API_KEY")
s3 = boto3.client("s3")



def transform_raw_data():
    """
    Returns a dict that contains username, user_id, github_url
    """
    prefix = f"{RAW_FOLDER}/github_users_raw"
    try:
        get_all_files = s3.list_objects_v2(Bucket = BUCKET_NAME, Prefix = prefix)
        
        if "Contents" not in get_all_files:
            print(f"No files exists in the bucket - {BUCKET_NAME}")
            return None
        
        sorted_files = sorted(get_all_files["Contents"], key=lambda x:x['LastModified'], reverse=True)
    
        file_path = sorted_files[0]["Key"]
        print(file_path)
        
        data = s3.get_object(Bucket = BUCKET_NAME, Key = file_path)
    except Exception as e:
        print(f"Error occured while gathering the data from S3 bucket {BUCKET_NAME}: ",e )
        raise ValueError(f"S3 access failed for bucket {BUCKET_NAME}")
    
    
    data_read = json.loads(data["Body"].read().decode("utf-8"))
    
    if not isinstance(data_read, list):
        print("Error : Expected list of users data but received another type")
        raise ValueError("Error: Expected list of user data but received another type")
    
    total_users = []
    headers = {"Accept": "application/vnd.github.v3+json", "Authorization": f"token {API_KEY}"}
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
            "user_api_url":user.get("url"),
            "followers_count": user_data.get("followers", 0),
            })
        except Exception as e:
            print(f"Error occurred while geeting user({user.get('login')}) info ", e)

    return total_users


def upload_transform_data():
    
    data = transform_raw_data()
    
    if data is not None:
        
    
        json_data = json.dumps(data)
        TRANSFORMED_PATH = f"{PROCESSED_FOLDER}/github_users_processed.json"
        
        upload_to_s3(json_data,TRANSFORMED_PATH )
    else:
        
        raise ValueError(f"Bucket has no such folders")


upload_transform_data()


    
    
    
