import pytest
from datetime import datetime, timedelta, timezone
from app.eshop import Product, ShoppingCart, Order
from services.repository import ShippingRepository
from services.publisher import ShippingPublisher
from services.service import ShippingService


# --- РІВЕНЬ 1: Інтеграція з БД (Repository + DynamoDB) ---

def test_1_repository_create_shipping(dynamo_resource):
    repo = ShippingRepository()
    due_date = datetime.now(timezone.utc) + timedelta(days=1)
    shipping_id = repo.create_shipping("Нова Пошта", ["ProdA", "ProdB"], "order_123", "created", due_date)

    assert shipping_id is not None
    item = repo.get_shipping(shipping_id)
    assert item["shipping_status"] == "created"
    assert item["order_id"] == "order_123"


def test_2_repository_update_status(dynamo_resource):
    repo = ShippingRepository()
    due_date = datetime.now(timezone.utc) + timedelta(days=1)
    shipping_id = repo.create_shipping("Укр Пошта", ["ProdC"], "order_124", "created", due_date)

    repo.update_shipping_status(shipping_id, "completed")
    item = repo.get_shipping(shipping_id)
    assert item["shipping_status"] == "completed"


# --- РІВЕНЬ 1: Інтеграція з Чергами (Publisher + SQS) ---

def test_3_publisher_send_and_poll(dynamo_resource):
    publisher = ShippingPublisher()
    test_shipping_id = "test_ship_uuid_999"

    msg_id = publisher.send_new_shipping(test_shipping_id)
    assert msg_id is not None

    messages = publisher.poll_shipping(batch_size=1)
    assert test_shipping_id in messages


# --- РІВЕНЬ 2: Інтеграція Сервісу з Репозиторієм та Паблішером ---

def test_4_service_create_shipping_integration(dynamo_resource):
    service = ShippingService(ShippingRepository(), ShippingPublisher())
    due_date = datetime.now(timezone.utc) + timedelta(days=1)

    shipping_id = service.create_shipping("Самовивіз", ["ProdX"], "order_200", due_date)

    # Перевірка, що статус в БД змінився на 'in progress'
    assert service.check_status(shipping_id) == ShippingService.SHIPPING_IN_PROGRESS


def test_5_service_process_shipping_success(dynamo_resource):
    service = ShippingService(ShippingRepository(), ShippingPublisher())
    due_date = datetime.now(timezone.utc) + timedelta(days=1)  # Актуальна дата
    shipping_id = service.create_shipping("Meest Express", ["ProdY"], "order_201", due_date)

    # Викликаємо обробку конкретного замовлення
    service.process_shipping(shipping_id)
    assert service.check_status(shipping_id) == ShippingService.SHIPPING_COMPLETED


def test_6_service_process_shipping_failed_due_to_timeout(dynamo_resource):
    repo = ShippingRepository()
    service = ShippingService(repo, ShippingPublisher())

    # Імітуємо замовлення з простроченою датою
    past_date = datetime.now(timezone.utc) - timedelta(days=1)
    shipping_id = repo.create_shipping("Нова Пошта", ["ProdZ"], "order_202", "in progress", past_date)

    service.process_shipping(shipping_id)
    assert service.check_status(shipping_id) == ShippingService.SHIPPING_FAILED


def test_7_service_process_batch_integration(dynamo_resource):
    service = ShippingService(ShippingRepository(), ShippingPublisher())
    due_date = datetime.now(timezone.utc) + timedelta(days=1)

    # Створюємо 2 відправки, вони потрапляють у чергу SQS
    id1 = service.create_shipping("Самовивіз", ["P1"], "O1", due_date)
    id2 = service.create_shipping("Самовивіз", ["P2"], "O2", due_date)

    # Обробляємо пачкою (читання з SQS + оновлення в DynamoDB)
    service.process_shipping_batch()

    assert service.check_status(id1) == ShippingService.SHIPPING_COMPLETED
    assert service.check_status(id2) == ShippingService.SHIPPING_COMPLETED


def test_8_service_invalid_shipping_type_raises_error(dynamo_resource):
    service = ShippingService(ShippingRepository(), ShippingPublisher())
    due_date = datetime.now(timezone.utc) + timedelta(days=1)

    with pytest.raises(ValueError, match="Shipping type is not available"):
        service.create_shipping("Неіснуюча Пошта", ["P3"], "O3", due_date)


# --- РІВЕНЬ 3: Повна інтеграція системи (Cart -> Order -> Service -> DB + SQS) ---

def test_9_order_placement_full_flow(dynamo_resource):
    service = ShippingService(ShippingRepository(), ShippingPublisher())
    cart = ShoppingCart()
    prod = Product("Laptop", 1000.0, 5)
    cart.add_product(prod, 1)

    order = Order(cart, service, "order_full_1")
    due_date = datetime.now(timezone.utc) + timedelta(days=2)

    shipping_id = order.place_order("Нова Пошта", due_date)

    # Перевіряємо, що кошик очистився
    assert len(cart.products) == 0
    # Перевіряємо доступність товару (було 5, стало 4)
    assert prod.available_amount == 4
    # Перевіряємо, що замовлення збережено в БД через сервіс
    assert service.check_status(shipping_id) == ShippingService.SHIPPING_IN_PROGRESS


def test_10_order_placement_with_invalid_date_fails(dynamo_resource):
    service = ShippingService(ShippingRepository(), ShippingPublisher())
    cart = ShoppingCart()
    cart.add_product(Product("Mouse", 50.0, 10), 2)
    order = Order(cart, service, "order_full_2")

    past_date = datetime.now(timezone.utc) - timedelta(minutes=5)

    with pytest.raises(ValueError, match="Shipping due datetime must be greater than datetime now"):
        order.place_order("Укр Пошта", past_date)