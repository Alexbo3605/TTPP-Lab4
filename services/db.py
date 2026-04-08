import boto3
from .config import AWS_ENDPOINT_URL, AWS_REGION

def get_dynamodb_resource():
    return boto3.resource(
        "dynamodb",
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
