from .config import SHIPPING_TABLE_NAME
from .db import get_dynamodb_resource

from uuid import uuid4
from datetime import datetime, timezone


class ShippingRepository:


    def __init__(self):
        dynamo_resource = get_dynamodb_resource()
        self.table = dynamo_resource.Table(SHIPPING_TABLE_NAME)


    def get_shipping(self, shipping_id):
        response = self.table.get_item(Key={"shipping_id": shipping_id})
        return response.get("Item")

    def create_shipping(self, shipping_type: str, product_ids: list, order_id: str, status: str, due_date: datetime):
        shipping_id = str(uuid4())
        item = {
            "shipping_id": shipping_id,
            "shipping_type": shipping_type,
            "order_id": order_id,
            "product_ids": ",".join(product_ids),
            "shipping_status": status,
            "created_date": datetime.now(timezone.utc).isoformat(),
            "due_date": due_date.replace(tzinfo=timezone.utc).isoformat()
        }
        self.table.put_item(Item=item)
        return shipping_id

    def update_shipping_status(self, shipping_id, status):
        response = self.table.update_item(
            Key={
                'shipping_id': shipping_id,
            },
            UpdateExpression='SET shipping_status = :sh_status',
            ExpressionAttributeValues={
                ':sh_status': status
            }
        )

        return response
