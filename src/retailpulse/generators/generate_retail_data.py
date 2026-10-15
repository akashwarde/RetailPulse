
import random
from datetime import date, datetime, timedelta
from pathlib import Path
import csv

SEED = 42
random.seed(SEED)

OUTPUT_DIR = Path("data/source")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Ishaan", "Arjun",
    "Ananya", "Diya", "Isha", "Meera", "Aditi",
]
LAST_NAMES = [
    "Sharma", "Patel", "Singh", "Gupta", "Kumar",
    "Shah", "Joshi", "Desai", "Rao", "Mehta",
]
CITIES = [
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"),
    ("Nashik", "Maharashtra"),
    ("Bengaluru", "Karnataka"),
    ("Hyderabad", "Telangana"),
    ("Delhi", "Delhi"),
    ("Jaipur", "Rajasthan"),
    ("Ahmedabad", "Gujarat"),
]
CATEGORIES = {
    "Electronics": ["Wireless Mouse", "Keyboard", "Headphones", "USB Hub"],
    "Home": ["Water Bottle", "Desk Lamp", "Storage Box", "Bedsheet"],
    "Fashion": ["T-Shirt", "Jeans", "Backpack", "Sneakers"],
    "Grocery": ["Coffee", "Rice Pack", "Cooking Oil", "Breakfast Cereal"],
}
PAYMENT_METHODS = ["UPI", "Card", "Cash", "Net Banking"]
ORDER_STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED"]


def write_csv(filename, headers, rows):
    with (OUTPUT_DIR / filename).open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


def generate():
    customers = []
    for cid in range(1, 501):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city, state = random.choice(CITIES)
        signup = date(2024, 1, 1) + timedelta(days=random.randint(0, 900))
        customers.append([
            cid, first, last, f"customer{cid}@example.com",
            city, state, signup.isoformat()
        ])

    products = []
    pid = 1
    for category, names in CATEGORIES.items():
        for name in names:
            products.append([
                pid, name, category,
                round(random.uniform(99, 15000), 2), True
            ])
            pid += 1

    stores = []
    for sid, (city, state) in enumerate(CITIES, start=1):
        stores.append([
            sid, f"{city} Store", city, state,
            date(2019, 1, 1).isoformat()
        ])

    orders = []
    order_items = []
    item_id = 1
    start = datetime(2025, 1, 1)

    for oid in range(1, 5001):
        customer_id = random.randint(1, 500)
        store_id = random.randint(1, len(stores))
        timestamp = start + timedelta(
            minutes=random.randint(0, 365 * 24 * 60 - 1)
        )
        status = random.choice(ORDER_STATUSES)

        orders.append([
            oid, customer_id, store_id,
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            status, random.choice(PAYMENT_METHODS)
        ])

        for _ in range(random.randint(1, 4)):
            product = random.choice(products)
            order_items.append([
                item_id, oid, product[0],
                random.randint(1, 5), product[3]
            ])
            item_id += 1

    write_csv(
        "customers.csv",
        ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"],
        customers,
    )
    write_csv(
        "products.csv",
        ["product_id", "product_name", "category", "unit_price", "active"],
        products,
    )
    write_csv(
        "stores.csv",
        ["store_id", "store_name", "city", "state", "opened_date"],
        stores,
    )
    write_csv(
        "orders.csv",
        ["order_id", "customer_id", "store_id", "order_timestamp", "order_status", "payment_method"],
        orders,
    )
    write_csv(
        "order_items.csv",
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
        order_items,
    )

    print(f"Generated {len(customers)} customers")
    print(f"Generated {len(products)} products")
    print(f"Generated {len(stores)} stores")
    print(f"Generated {len(orders)} orders")
    print(f"Generated {len(order_items)} order items")
    print(f"CSV files saved in: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    generate()
