import uuid


class Product:
    def __init__(self, name, price, available_amount):
        self.name = name
        self.price = price
        self.available_amount = available_amount

    def is_available(self, requested_amount):
        return self.available_amount >= requested_amount

    def buy(self, requested_amount):
        self.available_amount -= requested_amount

    def __eq__(self, other):
        return self.name == other.name

    def __ne__(self, other):
        return self.name != other.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name

class ShoppingCart:
    def __init__(self):
        self.products = dict()

    def contains_product(self, product):
        return product in self.products

    def calculate_total(self):
        return sum([p.price * count for p, count in self.products.items()])

    def add_product(self, product: Product, amount: int):
        if amount <= 0:
            raise ValueError("Amount must be greater than zero")
        if not product.is_available(amount):
            raise ValueError(f"Product {product} has only {product.available_amount} items")
        self.products[product] = amount

    def remove_product(self, product):
        if product in self.products:
            del self.products[product]


    def submit_cart_order(self):
        product_ids = []
        for product, count in self.products.items():
            product.buy(count)
            product_ids.append(str(product))
        self.products.clear()

        return product_ids

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from services import ShippingService

@dataclass
class Order:
    cart: ShoppingCart
    shipping_service: ShippingService
    order_id: str = str(uuid.uuid4())

    def place_order(self, shipping_type, due_date: datetime = None):
        if not due_date:
            due_date = datetime.now(timezone.utc) + timedelta(seconds=3)
        product_ids = self.cart.submit_cart_order()
        print(due_date)
        return self.shipping_service.create_shipping(shipping_type, product_ids, self.order_id, due_date)

@dataclass()
class Shipment:
    shipping_id: str
    shipping_service: ShippingService

    def check_shipping_status(self):
        return self.shipping_service.check_status(self.shipping_id)
