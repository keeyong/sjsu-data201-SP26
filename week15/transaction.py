import pymysql
import os
from pymysql import MySQLError

# ── CONFIG — update to match your environment ─────────────
DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": os.environ.get("MYSQL_PASSWORD"),
    "database": "shop",
    "cursorclass": pymysql.cursors.Cursor,  # default cursor
}


# ──────────────────────────────────────────────────────────
# 1. Connecting & the Cursor Pattern
# ──────────────────────────────────────────────────────────
def get_connection():
    """Return a live pymysql connection, or raise on failure."""
    conn = pymysql.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
    print(f"✅ Connected to MySQL {version}")
    return conn


def print_state(conn, label, product_id):
    """Print current orders count and product stock"""
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM orders")
        order_count = cursor.fetchone()[0]

        cursor.execute("SELECT stock FROM products WHERE id = %s", (product_id,))
        result = cursor.fetchone()
        stock = result[0] if result else None

    print(f"\n[{label}] orders={order_count}, product(product_id).stock={stock}")


def create_order(conn, customer_id, product_id, quantity):
    try:
        # Create a cursor to execute SQL queries
        with conn.cursor() as cursor:

            # 1. Insert a new order
            cursor.execute(
                """
                INSERT INTO orders (customer_id, product_id, quantity)
                VALUES (%s, %s, %s)
                """,
                (customer_id, product_id, quantity)
            )

            # 2. Decrease product stock
            cursor.execute(
                """
                UPDATE products
                SET stock = stock - %s
                WHERE id = %s AND stock >= %s
                """,
                (quantity, product_id, quantity)
            )

            # Check if the stock update actually happened
            # If no rows were affected, there was not enough stock
            if cursor.rowcount == 0:
                raise Exception("Insufficient stock")

        # If all queries succeed, commit the transaction
        conn.commit()
        print("Order completed (commit)")

    except Exception as e:
        # If any error occurs, roll back all changes
        conn.rollback()
        print(f"Transaction failed → rollback: {e}")


# ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    conn = None
    try:
        conn = get_connection()
        # BEFORE
        print_state(conn, "BEFORE", 3)        
        # Call the transaction function
        create_order(
            conn,
            customer_id=1,
            product_id=3,
            quantity=2
        )
        # AFTER
        print_state(conn, "AFTER", 3)        
    except MySQLError as e:
        print(f"MySQL error: {e}")
    finally:
        if conn:
            conn.close()
            print("Connection closed.")
