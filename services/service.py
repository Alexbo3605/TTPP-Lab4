from .repository import ShippingRepository
from .publisher import ShippingPublisher
from datetime import datetime, timezone


class ShippingService:
    SHIPPING_CREATED: str = 'created'
    SHIPPING_IN_PROGRESS: str = 'in progress'
    SHIPPING_COMPLETED: str = 'completed'
    SHIPPING_FAILED: str = 'failed'

    def __init__(self, repository, publisher):
        self.repository = repository
        self.publisher = publisher

    @staticmethod
    def list_available_shipping_type():
        return ['Нова Пошта', 'Укр Пошта', 'Meest Express', 'Самовивіз']

    def create_shipping(self, shipping_type, product_ids, order_id, due_date):
        if shipping_type not in self.list_available_shipping_type():
            raise ValueError("Shipping type is not available")

        if due_date <= datetime.now(timezone.utc):
            raise ValueError("Shipping due datetime must be greater than datetime now")

        shipping_id = self.repository.create_shipping(shipping_type, product_ids, order_id, self.SHIPPING_CREATED, due_date)

        self.publisher.send_new_shipping(shipping_id)
        self.repository.update_shipping_status(shipping_id, self.SHIPPING_IN_PROGRESS)

        return shipping_id

    def process_shipping_batch(self):
        result = []
        shipping = self.publisher.poll_shipping()
        for shipping_id in shipping:
            shipping = self.process_shipping(shipping_id)
            result.append(shipping)

        return result

    def process_shipping(self, shipping_id):
        shipping = self.repository.get_shipping(shipping_id)
        if datetime.fromisoformat(shipping['due_date']) < datetime.now(timezone.utc):
            return self.fail_shipping(shipping_id)

        return self.complete_shipping(shipping_id)

    def check_status(self, shipping_id):
        shipping = self.repository.get_shipping(shipping_id)

        return shipping['shipping_status']

    def fail_shipping(self, shipping_id):
        response = self.repository.update_shipping_status(shipping_id, self.SHIPPING_FAILED)
        return response['ResponseMetadata']

    def complete_shipping(self, shipping_id):
        response = self.repository.update_shipping_status(shipping_id, self.SHIPPING_COMPLETED)
        return response['ResponseMetadata']
