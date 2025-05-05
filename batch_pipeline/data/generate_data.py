import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os
from minio import Minio
from dagster import asset, Definitions, ScheduleDefinition, define_asset_job

# -----------------------------
# Константы
# -----------------------------
fake = Faker('ru_RU')
TODAY = datetime.today().date()

# Параметры подключения к MinIO
MINIO_CONFIG = {
    "endpoint": "localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "secure": False,
    "bucket_name": "daily-sales-data"
}

# Папки для хранения статичных и дневных данных
STATIC_DIR = "static"
DAILY_DIR = f"daily/{TODAY}"

os.makedirs("tmp", exist_ok=True)  # временная папка для csv перед загрузкой

# -----------------------------
# Функция создания клиента MinIO
# -----------------------------
def get_minio_client():
    return Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=MINIO_CONFIG["secure"]
    )

# -----------------------------
# Создание бакета (если не существует)
# -----------------------------
def ensure_bucket_exists(client):
    bucket_name = MINIO_CONFIG["bucket_name"]
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    return bucket_name

# -----------------------------
# Загрузка DataFrame в MinIO
# -----------------------------
def upload_df_to_minio(df: pd.DataFrame, object_path: str):
    client = get_minio_client()
    bucket_name = ensure_bucket_exists(client)

    temp_file = f"tmp/{object_path.replace('/', '_')}.csv"
    df.to_csv(temp_file, index=False)
    client.fput_object(bucket_name, object_path, temp_file)
    os.remove(temp_file)
    return f"Uploaded to MinIO: {object_path}"

# -----------------------------
# 1. dim_customers - Клиенты (разовое создание)
# -----------------------------
def generate_customers(n=10000):
    countries = ['Россия', 'Украина', 'Казахстан', 'Беларусь', 'Армения', 'Грузия', 'Таджикистан']
    customers = []
    for i in range(n):
        customers.append({
            "customer_id": i + 1,
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "country": random.choice(countries)
        })
    return pd.DataFrame(customers)

@asset
def dim_customers():
    try:
        client = get_minio_client()
        bucket = MINIO_CONFIG["bucket_name"]
        path = f"{STATIC_DIR}/dim_customers.csv"
        temp_file = "tmp/dim_customers.csv"
        client.fget_object(bucket, path, temp_file)
        return pd.read_csv(temp_file)
    except Exception as e:
        print(f"dim_customers не найден в MinIO: {e}")
        df = generate_customers()
        upload_df_to_minio(df, path)
        return df

# -----------------------------
# 2. dim_products - Продукты (разовое создание)
# -----------------------------
def generate_products(n=100):
    categories = ['Электроника', 'Одежда', 'Косметика', 'Книги', 'Продукты']
    products = []
    for i in range(n):
        products.append({
            "product_id": i + 1,
            "product_name": fake.word().capitalize() + " " + random.choice(["Pro", "Lite", "Max", "Ultra"]),
            "category": random.choice(categories),
            "price": round(random.uniform(100, 10000), 2)
        })
    return pd.DataFrame(products)

@asset
def dim_products():
    try:
        client = get_minio_client()
        bucket = MINIO_CONFIG["bucket_name"]
        path = f"{STATIC_DIR}/dim_products.csv"
        temp_file = "tmp/dim_products.csv"
        client.fget_object(bucket, path, temp_file)
        return pd.read_csv(temp_file)
    except Exception as e:
        print(f"dim_products не найден в MinIO: {e}")
        df = generate_products()
        upload_df_to_minio(df, path)
        return df

# -----------------------------
# 3. fact_orders / fact_sales - Ежедневная генерация
# -----------------------------
def generate_orders_and_sales(dim_customers, dim_products, n_orders=1000):
    orders = []
    sales = []

    customer_ids = dim_customers['customer_id'].tolist()
    product_ids = dim_products['product_id'].tolist()
    product_prices = dict(zip(dim_products['product_id'], dim_products['price']))

    # Статусы заказов
    order_statuses = ['processing', 'shipped', 'delivered', 'cancelled']

    # Генерация времени покупки (рандомное время сегодня)
    def random_time_today():
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return datetime.combine(TODAY, datetime.min.time()) + timedelta(hours=hour, minutes=minute, seconds=second)

    line_item_id = 1

    for order_id in range(1, n_orders + 1):
        customer_id = random.choice(customer_ids)
        order_datetime = random_time_today()
        n_items = random.randint(1, 5)

        total_amount = 0
        items_in_order = []

        for _ in range(n_items):
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 3)
            price = product_prices[product_id]
            item_total = round(price * quantity, 2)
            total_amount += item_total

            items_in_order.append({
                "line_item_id": line_item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "item_total": item_total
            })
            line_item_id += 1

        status = random.choice(order_statuses)

        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_datetime": order_datetime,
            "total_amount": round(total_amount, 2),
            "item_count": n_items,
            "status": status
        })

        sales.extend(items_in_order)

    return pd.DataFrame(orders), pd.DataFrame(sales)

@asset(deps=[dim_customers, dim_products])
def fact_orders(dim_customers, dim_products):
    orders_df, _ = generate_orders_and_sales(dim_customers, dim_products)
    return orders_df

@asset(deps=[dim_customers, dim_products])
def fact_sales(dim_customers, dim_products):
    _, sales_df = generate_orders_and_sales(dim_customers, dim_products)
    return sales_df

# -----------------------------
# 4. Сохранение данных в MinIO
# -----------------------------
@asset(deps=[dim_customers, dim_products, fact_orders, fact_sales])
def save_daily_data(dim_customers, dim_products, fact_orders, fact_sales):
    upload_df_to_minio(dim_customers, f"{STATIC_DIR}/dim_customers.csv")
    upload_df_to_minio(dim_products, f"{STATIC_DIR}/dim_products.csv")
    upload_df_to_minio(fact_orders, f"{DAILY_DIR}/fact_orders.csv")
    upload_df_to_minio(fact_sales, f"{DAILY_DIR}/fact_sales.csv")

    return f"✅ Данные за {TODAY} успешно загружены в MinIO."

# -----------------------------
# Job и расписание
# -----------------------------
daily_data_job = define_asset_job(name="daily_data_generation_job", selection="*")

daily_schedule = ScheduleDefinition(
    job=daily_data_job,
    cron_schedule="0 0 * * *"  # Ежедневно в полночь
)

# -----------------------------
# Запуск Dagster
# -----------------------------
defs = Definitions(
    assets=[dim_customers, dim_products, fact_orders, fact_sales, save_daily_data],
    schedules=[daily_schedule]
)