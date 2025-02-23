import json
import boto3

s3_client = boto3.client("s3")

def upload_to_s3(data, key, bucket):
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
            raise ValueError("Unsupported Data Format, expected JSON string or dictionary")
        
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType='application/json'
        )
        print(f"Successfully uploaded to S3 bucket {bucket}")

    except Exception as e:
        print(f"Error Uploading to S3:", e)
