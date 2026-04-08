import os

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SHIPPING_TABLE_NAME = os.getenv("SHIPPING_TABLE_NAME", "ShippingTable")
SHIPPING_QUEUE = os.getenv("SHIPPING_QUEUE_NAME", "ShippingQueue")
