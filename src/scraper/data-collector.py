import os
import json
import boto3
from dotenv import load_dotenv
import time
import requests
from ..utils.s3_helper import upload_to_s3
from ..utils.s3_config import RAW_FOLDER
from datetime import datetime
from ..utils.log_config import logger

load_dotenv()
API_KEY = os.getenv("GITHUB_API_KEY")



def fetch_github_data(pages=2,per_pages=40):
    
    """
    returns github users list who has 500+ followers
    
    param pages: number of pages want to fetch
    param per_pages: each page total user count
    
    """
    
    if not API_KEY:
        logger.error("GitHub API key is missing, Please create an environment variable file and add as below \nGITHUB_API_KEY = 'Your Personal Access Key'")
        raise ValueError("GitHub API Key is Missing")
    
    users_list = []
    base_url = f"https://api.github.com/search/users?q=followers:>500&sort=followers&"
    headers = {"Accept": "application/vnd.github.v3+json", "Authorization": f"token {API_KEY}"}
    for page in range(1, pages + 1):
    
        url = base_url + f"page={page}&per_page={per_pages}"
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            
            data = response.json().get("items", [])
            users_list.extend(data)
            logger.info(f"Fetched {len(data)} users from page {page}")
            
        elif response.status_code == 403 :
            
            logger.warning("Rate Limit Error (403). Sleeping for 60 seconds before retrying...")
            time.sleep(60)
        else:
            
            logger.error(f"Failed to fetch {page}: ", response.status_code)
            raise RuntimeError(f"API request failed with status code {response.status_code}")
    
    
    return users_list
    


def upload_raw_data():
    
    """Fetches GitHub data and uploads it to S3."""
    try:
        github_data = json.dumps(fetch_github_data())
        
        file_path = f"{RAW_FOLDER}/github_users_raw_{datetime.now().strftime(r'%Y%m%d_%H%M%S')}.json"
        upload_to_s3(github_data, file_path)
        logger.info(f"Uploaded raw data to S3: {file_path}")
    except Exception as e:
        logger.error(f"Failed to upload data to S3: {str(e)}")



if __name__ == "__main__":

    upload_raw_data()