import json
import boto3
import uuid
import datetime

s3_client = boto3.client("s3")

BUCKET_NAME = "lks-serverless-ml-raw"

def lambda_handler(event, context):
    try:
        # Parse JSON body
        body = json.loads(event["body"])
        
        # Generate a unique file name
        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"requests/{timestamp}_{uuid.uuid4()}.json"
        
        # Upload JSON to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(body),
            ContentType="application/json"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data uploaded successfully!", "file": file_name})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
