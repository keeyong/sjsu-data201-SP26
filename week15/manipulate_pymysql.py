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


# ──────────────────────────────────────────────────────────
# 2. SELECT — fetchall vs fetchone
# ──────────────────────────────────────────────────────────
def demo_select(conn):
    print("\n── SELECT ─────────────────────────────────────────")

    with conn.cursor() as cursor:
        # fetchall() → tuple of tuples
        cursor.execute("SELECT id, name, city FROM customers")
        rows = cursor.fetchall()
        print(f"fetchall() returned {len(rows)} rows:")
        for row in rows:
            print(f"  id={row[0]:2d}  name={row[1]:<15s}  city={row[2]}")

        # fetchone() — one row at a time
        cursor.execute("SELECT * FROM customers WHERE city = %s", ("San Jose",))
        print("\nfetchone() — San Jose customers:")
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            print(f"  {row}")

    # dictionary cursor — access columns by name
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("SELECT id, product, amount, status FROM orders LIMIT 5")
        print("\nDictionary cursor:")
        for row in cursor.fetchall():
            print(f"  #{row['id']}  {row['product']:<25s}  ${row['amount']:.2f}  [{row['status']}]")
            

# ──────────────────────────────────────────────────────────
# 3. Parameterized Queries — SQL Injection Defense
# ──────────────────────────────────────────────────────────
def demo_parameterized(conn):
    print("\n── PARAMETERIZED QUERY ────────────────────────────")

    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        status_filter = "shipped"   # imagine this came from user input

        # Always use %s placeholders — NEVER f-strings in SQL
        cursor.execute(
            "SELECT id, product, amount FROM orders WHERE status = %s",
            (status_filter,)          # ← must be a tuple, even for one value
        )
        rows = cursor.fetchall()
        print(f"Orders with status='{status_filter}':")
        for r in rows:
            print(f"  #{r['id']}  {r['product']:<25s}  ${r['amount']:.2f}")

        # Multiple parameters
        cursor.execute(
            "SELECT * FROM orders WHERE amount > %s AND status = %s",
            (50.0, "shipped")
        )
        print(f"\nShipped orders over $50: {len(cursor.fetchall())} found")


# ──────────────────────────────────────────────────────────
# 4. INSERT with commit()
# ──────────────────────────────────────────────────────────
def demo_insert(conn):
    print("\n── INSERT ─────────────────────────────────────────")

    new_order = {
        "customer_id": 1,
        "product":     "Python Textbook",
        "amount":      59.99,
        "status":      "pending",
    }

    sql = """
        INSERT INTO orders (customer_id, product, amount, status)
        VALUES (%(customer_id)s, %(product)s, %(amount)s, %(status)s)
    """

    with conn.cursor() as cursor:
        cursor.execute(sql, new_order)
        conn.commit()                         # ← required to persist!
        print(f"Inserted order with id={cursor.lastrowid}")


# ──────────────────────────────────────────────────────────
# 5. UPDATE
# ──────────────────────────────────────────────────────────
def demo_update(conn):
    print("── UPDATE ─────────────────────────────────────────")

    with conn.cursor() as cursor:
        # Update the order we just inserted
        cursor.execute(
            "UPDATE orders SET status = %s WHERE status = %s AND product = %s",
            ("shipped", "pending", "Python Textbook")
        )
        conn.commit()
        print(f"Rows affected: {cursor.rowcount}")


# ──────────────────────────────────────────────────────────
# 6. DELETE with rollback() demo
# ──────────────────────────────────────────────────────────
def demo_delete_with_rollback(conn):
    print("\n── DELETE + ROLLBACK ──────────────────────────────")

    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM orders")
        before = cursor.fetchone()[0]
        print(f"Orders before delete: {before}")

        # Delete the test order
        cursor.execute(
            "DELETE FROM orders WHERE product = %s",
            ("Python Textbook",)
        )
        print(f"Deleted {cursor.rowcount} row(s) — NOT committed yet")

        # Simulate a problem → rollback
        conn.rollback()
        print("Rolled back! Nothing was deleted.")

        cursor.execute("SELECT COUNT(*) FROM orders")
        after = cursor.fetchone()[0]
        print(f"Orders after rollback: {after}")


# ──────────────────────────────────────────────────────────
# 7. executemany() — bulk inserts
# ──────────────────────────────────────────────────────────
def demo_bulk_insert(conn):
    print("\n── BULK INSERT (executemany) ──────────────────────")

    batch = [
        (2, "USB Cable",      9.99, "pending"),
        (3, "HDMI Adapter",  19.99, "pending"),
        (4, "SD Card 128GB", 29.99, "shipped"),
    ]
    sql = "INSERT INTO orders (customer_id, product, amount, status) VALUES (%s, %s, %s, %s)"

    with conn.cursor() as cursor:
        cursor.executemany(sql, batch)
        conn.commit()
        print(f"Inserted {cursor.rowcount} rows in one call")


# ──────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    conn = None
    try:
        conn = get_connection()
        demo_select(conn)
        demo_parameterized(conn)
        demo_insert(conn)
        demo_update(conn)
        demo_delete_with_rollback(conn)
        demo_bulk_insert(conn)
        print("Lab 1 complete!")
    except MySQLError as e:
        print(f"MySQL error: {e}")
    finally:
        if conn:
            conn.close()
            print("Connection closed.")
