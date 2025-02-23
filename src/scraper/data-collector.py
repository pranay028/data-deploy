import os
import json
import boto3
from dotenv import load_dotenv
import time
import requests
from ..utils.s3_config import BUCKET_NAME, RAW_FOLDER

# Load API Key
load_dotenv()
API_KEY = os.getenv("GITHUB_API_KEY")
s3 = boto3.client("s3")

# Get GitHub Data
def fetch_github_data(pages=3,per_pages=40):
    
    users_list = []
    base_url = f"https://api.github.com/search/users?q=followers:>1000&sort=followers&"
    headers = {"Accept": "application/vnd.github.v3+json", "Authorization": f"token {API_KEY}"}
    for page in (1, pages + 1):
    
        url = base_url + f"page={page}&per_page={per_pages}"
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            
            data = response.json().get("items", [])
            users_list.extend(data)
            
            return users_list
        elif response.status_code == 403 :
            
            print("RateLimitError - Status code : ", response.status_code)
            print("sleeping for 60 sec")
            time.sleep(60)
        else:
            
            print(f"Failed to fetch {page}: ", response.status_code)
    

# Upload to S3 (Raw Data)
def upload_to_s3(data, file_name):
    file_path = f"{RAW_FOLDER}{file_name}"
   
    s3.put_object(Bucket=BUCKET_NAME, Key=file_path, Body=json.dumps(data), ContentType= "application/json")
    print(f" Uploaded: {file_path}")


# Fetch & Upload
github_data = fetch_github_data()
upload_to_s3(github_data, "github_users_raw.json")
