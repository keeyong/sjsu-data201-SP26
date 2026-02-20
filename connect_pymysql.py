"""
Simple MySQL Test Script using PyMySQL
pip install pymysql
"""
import pymysql
import os

password = os.environ.get("MYSQL_PASSWORD")

# 1. Connect
conn = pymysql.connect(
    host="localhost",
    user="root",
    password=password,
    cursorclass=pymysql.cursors.DictCursor  # rows returned as dicts
)

with conn:
    with conn.cursor() as cursor:

        # 2. Create database & table
        cursor.execute("CREATE DATABASE IF NOT EXISTS test_db")
        cursor.execute("USE test_db")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id    INT AUTO_INCREMENT PRIMARY KEY,
                name  VARCHAR(50),
                age   INT
            )
        """)

        # 3. Insert
        cursor.execute("INSERT INTO users (name, age) VALUES (%s, %s)", ("Alice", 30))
        cursor.execute("INSERT INTO users (name, age) VALUES (%s, %s)", ("Bob", 25))
        conn.commit()
        print("Inserted 2 rows.")

        # 4. Select — rows come back as dicts thanks to DictCursor
        cursor.execute("SELECT * FROM users")
        for row in cursor.fetchall():
            print(row)

        # 5. Update
        cursor.execute("UPDATE users SET age = %s WHERE name = %s", (31, "Alice"))
        conn.commit()
        print("Updated Alice's age.")

        # 6. Delete
        cursor.execute("DELETE FROM users WHERE name = %s", ("Bob",))
        conn.commit()
        print("Deleted Bob.")

        # 7. Final state
        cursor.execute("SELECT * FROM users")
        print("Final:", cursor.fetchall())

        # 8. Cleanup
        cursor.execute("DROP DATABASE test_db")

print("Done.")
