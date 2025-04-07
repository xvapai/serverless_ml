import json
import boto3
import pickle
import pandas as pd
import logging
from io import BytesIO
import decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
S3_CLIENT = boto3.client("s3")
DYNAMODB = boto3.resource("dynamodb")
SNS_CLIENT = boto3.client("sns")

# S3 Buckets
S3_BUCKET_MODEL = "abcdefg"
S3_BUCKET_RAW = "abcdefg"
S3_BUCKET_OUTPUT = "abcdefg"

# DynamoDB Table Name
DYNAMODB_TABLE = "abcdefg"

# SNS Topic ARN (replace with your actual ARN)
SNS_TOPIC_ARN = "abcdefg"

# Model Path
MODEL_PATH = "abcdefg"

def load_model():
    """Download and load model from S3"""
    s3_model_key = "abcdefg.pkl"
    S3_CLIENT.download_file(S3_BUCKET_MODEL, s3_model_key, MODEL_PATH)
    
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    
    return model

MODEL = load_model()  # Load model once during cold start

def read_s3_json(bucket, key):
    """Read JSON file from S3"""
    response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    return json.loads(content)

def get_prediction_class(price):
    """Classify prediction price into categories"""
    if price < 3000000:
        return 1
    elif 3000000 <= price < 5000000:
        return 2
    else:
        return 3

def store_to_dynamodb(record_id, data_point, predicted_price, prediction_class):
    """Store prediction result in DynamoDB"""
    table = DYNAMODB.Table(DYNAMODB_TABLE)

    # Ensure recordId is a string
    record_id = str(record_id)

    # Convert float to Decimal
    predicted_price_decimal = decimal.Decimal(str(predicted_price))  # Convert float to string first to avoid precision issues

    table.put_item(
        Item={
            "recordId": record_id,
            "features": json.dumps(data_point),
            "predicted_price": predicted_price_decimal,  # Use Decimal type
            "prediction_class": prediction_class
        }
    )
    logger.info(f"Stored record {record_id} in DynamoDB")


def send_sns_notification(error_message):
    """Send SNS notification on error"""
    SNS_CLIENT.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=f"Lambda Function Error: {error_message}",
        Subject="Lambda Error Notification"
    )
    logger.info("SNS notification sent.")

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    try:
        for record in event["Records"]:
            body = json.loads(record["body"]) if isinstance(record["body"], str) else record["body"]
            
            logger.info(f"Processing record: {json.dumps(body)}")

            # Extract S3 event details
            s3_event = body.get("Records", [])[0]
            s3_bucket = s3_event["s3"]["bucket"]["name"]
            s3_key = s3_event["s3"]["object"]["key"]

            logger.info(f"Reading input file from S3: {s3_bucket}/{s3_key}")

            # Read input JSON file from S3
            input_data = read_s3_json(s3_bucket, s3_key)

            # Extract features
            data_point = input_data.get("features", {})
            if not data_point:
                error_message = f"Missing 'features' in input file: {s3_key}"
                logger.error(error_message)
                send_sns_notification(error_message)  # Send SNS notification for missing features
                raise ValueError(error_message)

            # Perform prediction
            predicted_price = MODEL.predict(pd.DataFrame([data_point]))[0]
            prediction_class = get_prediction_class(predicted_price)

            response_data = {
                "input": data_point,
                "predicted_price": predicted_price,
                "prediction_class": prediction_class
            }

            # Store result in S3
            output_s3_key = f"predictions/{record['messageId']}.json"
            S3_CLIENT.put_object(
                Bucket=S3_BUCKET_OUTPUT,
                Key=output_s3_key,
                Body=json.dumps(response_data)
            )
            
            logger.info(f"Prediction stored in S3: {output_s3_key}")

            # Store result in DynamoDB
            store_to_dynamodb(record["messageId"], data_point, predicted_price, prediction_class)

            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Prediction stored in S3 & DynamoDB", "result": response_data}),
            }

    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        send_sns_notification(str(e))  # Send SNS notification on error
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
