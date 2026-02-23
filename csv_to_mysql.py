import requests
import pymysql
import os


def create_mysql_cur(host, user, password):
    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        cursorclass=pymysql.cursors.DictCursor  # rows returned as dicts
    )
    cur = conn.cursor()
    return cur


def extract(url):
    f = requests.get(url)
    return (f.text)


def transform(text):
    lines = text.strip().split("\n")
    records = []
    for l in lines:  # remove the first row
        (country, capital) = l.split(",")
        records.append([country, capital])
    # First remove the header
    return records[1:]


def load(con, database, table, records):
    target_table = f"{database}.{table}"
    try:
        con.execute("BEGIN;")
        con.execute(f"""CREATE DATABASE IF NOT EXISTS {database};""")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
          country varchar(128) primary key,
          capital varchar(128) not null);""")
        con.execute(f"""DELETE FROM {target_table}""")
        for r in records:
            country = r[0].replace("'", "''")
            capital = r[1].replace("'", "''")
            print(country, "-", capital)

            sql = f"INSERT INTO {target_table} (country, capital) VALUES ('{country}', '{capital}')"
            con.execute(sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


url = "https://s3-geospatial.s3.us-west-2.amazonaws.com/country_capital.csv"
text = extract(url)
lines = transform(text)
# ---------------------
password = os.environ.get("MYSQL_PASSWORD")
cur = create_mysql_cur("localhost", "root", password)
load(cur, "dev", "country_capital", lines)
