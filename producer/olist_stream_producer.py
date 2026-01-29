import json
import threading
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---- Master Data Pools ----
customers = {}
products = {}
sellers = {}
geolocations = {}

# ---- Helpers ----
def send(topic, data):
    producer.send(topic, value=data)
    print(f"[{topic}] -> {data}")

# ---- Static-like Streams (slow) ----
def generate_customer():
    cid = fake.uuid4()
    zip_code = str(random.randint(10000, 99999))
    customers[cid] = zip_code

    data = {
        "customer_id": cid,
        "customer_name": fake.name(),
        "customer_city": fake.city(),
        "zip_code_prefix": zip_code
    }
    send("olist_customers", data)

def generate_seller():
    sid = fake.uuid4()
    zip_code = str(random.randint(10000, 99999))
    sellers[sid] = zip_code

    data = {
        "seller_id": sid,
        "seller_name": fake.company(),
        "zip_code_prefix": zip_code
    }
    send("olist_sellers", data)

def generate_product():
    pid = fake.uuid4()
    products[pid] = True

    data = {
        "product_id": pid,
        "product_name": fake.word(),
        "category": random.choice(["electronics","fashion","books","home"])
    }
    send("olist_products", data)

def generate_geolocation(zip_code):
    data = {
        "zip_code_prefix": zip_code,
        "city": fake.city(),
        "state": fake.state()
    }
    send("olist_geolocation", data)

# ---- Transaction Streams (fast) ----
def generate_order():
    if not customers:
        return

    oid = fake.uuid4()
    cid = random.choice(list(customers.keys()))

    order = {
        "order_id": oid,
        "customer_id": cid,
        "order_status": random.choice(["created","approved","shipped","delivered"]),
        "order_time": datetime.now().isoformat()
    }
    send("olist_orders", order)

    # order_items
    for _ in range(random.randint(1,3)):
        pid = random.choice(list(products.keys()))
        sid = random.choice(list(sellers.keys()))

        item = {
            "order_id": oid,
            "product_id": pid,
            "seller_id": sid,
            "price": round(random.uniform(100, 5000), 2)
        }
        send("olist_order_items", item)

    # payment
    '''payment = {
        "order_id": oid,
        "payment_type": random.choice(["credit_card","upi","debit_card"]),
        

        "payment_value": round(random.uniform(100, 5000), 2)
    }
    send("olist_payments", payment)'''

    # review
    review = {
        "order_id": oid,
        "review_score": random.randint(1,5),
        "review_comment": fake.sentence()
    }
    send("olist_reviews", review)
    
    # fire payment asynchronously
    threading.Thread(
        target=send_payment_with_delay,
        args=(oid,)
    ).start()
    

def send_payment_with_delay(order_id):
    delay_seconds = random.randint(0, 300)  # 0–5 minutes

    time.sleep(delay_seconds)

    payment = {
        "order_id": order_id,
        "payment_type": random.choice(["credit_card", "upi", "debit_card"]),
        "payment_value": round(random.uniform(100, 5000), 2),
        "payment_time": datetime.now().isoformat()
    }

    send("olist_payments", payment)




# ---- MAIN LOOP ----
print("Starting Olist real-time simulator...")

while True:

    # Slowly generate master data
    if len(customers) < 20:
        generate_customer()
        generate_geolocation(list(customers.values())[-1])

    if len(sellers) < 10:
        generate_seller()
        generate_geolocation(list(sellers.values())[-1])

    if len(products) < 15:
        generate_product()

    # Fast transactional data
    generate_order()

    time.sleep(1)