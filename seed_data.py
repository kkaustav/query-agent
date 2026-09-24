import sqlite3
import os

def create_database():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect("data/ecommerce.db")
    cursor = conn.cursor()

    cursor.executescript("""
    DROP TABLE IF EXISTS order_items;
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS products;
    DROP TABLE IF EXISTS customers;

    CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        region TEXT NOT NULL,
        signup_date DATE
    );

    CREATE TABLE products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT NOT NULL,
        category TEXT NOT NULL,
        price REAL NOT NULL,
        stock_quantity INTEGER NOT NULL
    );

    CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        order_date DATE NOT NULL,
        status TEXT NOT NULL,
        total_amount REAL NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );

    CREATE TABLE order_items (
        item_id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price REAL NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );

    INSERT INTO customers VALUES
    (1, 'Alice Walker', 'alice@acme.com', 'North America', '2023-01-15'),
    (2, 'Devon Patel', 'devon@techcorp.io', 'Europe', '2023-02-20'),
    (3, 'Hiroshi Tanaka', 'hiroshi@global.jp', 'Asia-Pacific', '2023-03-12'),
    (4, 'Maria Gomez', 'maria@latam.org', 'South America', '2023-04-05'),
    (5, 'Liam Smith', 'liam@cloudnet.co.uk', 'Europe', '2023-05-18');

    INSERT INTO products VALUES
    (101, 'Cloud Data Lakehouse License', 'Software', 1200.00, 50),
    (102, 'Enterprise AI Copilot Seat', 'SaaS', 450.00, 200),
    (103, 'Vector Search Optimization Pack', 'Add-on', 150.00, 100),
    (104, 'Observability Agent Node', 'Infrastructure', 80.00, 500),
    (105, 'Developer Sandbox Pass', 'Subscription', 25.00, 1000);

    INSERT INTO orders VALUES
    (1001, 1, '2024-01-10', 'Completed', 1650.00),
    (1002, 2, '2024-01-15', 'Completed', 450.00),
    (1003, 3, '2024-02-01', 'Completed', 2400.00),
    (1004, 1, '2024-02-18', 'Completed', 80.00),
    (1005, 4, '2024-03-05', 'Pending', 300.00),
    (1006, 5, '2024-03-12', 'Completed', 1350.00);

    INSERT INTO order_items VALUES
    (1, 1001, 101, 1, 1200.00),
    (2, 1001, 102, 1, 450.00),
    (3, 1002, 102, 1, 450.00),
    (4, 1003, 101, 2, 1200.00),
    (5, 1004, 104, 1, 80.00),
    (6, 1005, 103, 2, 150.00),
    (7, 1006, 102, 3, 450.00);
    """)

    conn.commit()
    conn.close()
    print("Database data/ecommerce.db seeded successfully.")

if __name__ == "__main__":
    create_database()