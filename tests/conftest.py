import pytest
import boto3
from services.config import *
from services.db import get_dynamodb_resource


os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = AWS_REGION

@pytest.fixture(scope="session", autouse=True)
def setup_localstack_resources():
    dynamo_client = boto3.client(
        "dynamodb",
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION
    )
    existing_tables = dynamo_client.list_tables()["TableNames"]
    if SHIPPING_TABLE_NAME not in existing_tables:
        dynamo_client.create_table(
            TableName=SHIPPING_TABLE_NAME,
            KeySchema=[{"AttributeName": "shipping_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "shipping_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        dynamo_client.get_waiter("table_exists").wait(TableName=SHIPPING_TABLE_NAME)
    sqs_client = boto3.client(
        "sqs",
        endpoint_url=AWS_ENDPOINT_URL, region_name=AWS_REGION
    )
    response = sqs_client.create_queue(QueueName=SHIPPING_QUEUE)
    queue_url = response["QueueUrl"]

    yield  # Всі тести йдуть тут

    dynamo_client.delete_table(TableName=SHIPPING_TABLE_NAME)
    sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.fixture
def dynamo_resource():
    return get_dynamodb_resource()
