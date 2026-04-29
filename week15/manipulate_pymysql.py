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
        cursor.execute("SELECT id, product_id, quantity, status FROM orders LIMIT 5")
        print("\nDictionary cursor:")
        for row in cursor.fetchall():
            print(f"  #{row['id']}  {row['product_id']}  ${row['quantity']}  [{row['status']}]")
            

# ──────────────────────────────────────────────────────────
# 3. Parameterized Queries — SQL Injection Defense
# ──────────────────────────────────────────────────────────
def demo_parameterized(conn):
    print("\n── PARAMETERIZED QUERY ────────────────────────────")

    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        status_filter = "shipped"   # imagine this came from user input

        # Always use %s placeholders — NEVER f-strings in SQL
        cursor.execute(
            "SELECT id, product_id, quantity FROM orders WHERE status = %s",
            (status_filter,)          # ← must be a tuple, even for one value
        )
        rows = cursor.fetchall()
        print(f"Orders with status='{status_filter}':")
        for r in rows:
            print(f"  #{r['id']}  {r['product_id']}  ${r['quantity']}")

        # Multiple parameters
        cursor.execute(
            "SELECT * FROM orders WHERE quantity > %s AND status = %s",
            (2, "shipped")
        )
        print(f"\nShipped orders over 2 times: {len(cursor.fetchall())} found")


# ──────────────────────────────────────────────────────────
# 4. INSERT with commit()
# ──────────────────────────────────────────────────────────
def demo_insert(conn):
    print("\n── INSERT ─────────────────────────────────────────")

    new_order = {
        "customer_id": 1,
        "product_id":  1,
        "quantity":    3,
        "status":      "pending",
    }

    sql = """
        INSERT INTO orders (customer_id, product_id, quantity, status)
        VALUES (%(customer_id)s, %(product_id)s, %(quantity)s, %(status)s)
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
            "UPDATE orders SET status = %s WHERE status = %s AND product_id = %s",
            ("shipped", "pending", 2)
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
            "DELETE FROM orders WHERE product_id = %s",
            (1,)
        )
        print(f"Deleted {cursor.rowcount} row(s) — NOT committed yet")

        # Simulate a problem → rollback
        conn.rollback()
        print("Rolled back! Nothing was deleted.")

        cursor.execute("SELECT COUNT(*) FROM orders")
        after = cursor.fetchone()[0]
        print(f"Orders after rollback: {after}")



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
        print("Lab 1 complete!")
    except MySQLError as e:
        print(f"MySQL error: {e}")
    finally:
        if conn:
            conn.close()
            print("Connection closed.")
