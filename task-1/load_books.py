import psycopg2
import re
import json

DB = "dbname=task_1_books user=postgres password=60891705 host=localhost"


def ruby_to_json(text):
    text = text.strip()

    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]

    text = re.sub(r":(\w+)=>", r'"\1":', text)

    text = "[" + text + "]"

    return text


def parse_price(price_str):
    price_str = price_str.strip()

    if price_str.startswith("$"):
        return "USD", float(price_str.replace("$", ""))

    if price_str.startswith("€"):
        return "EUR", float(price_str.replace("€", ""))

    return None, None


raw = open("task1_d.json", "r", encoding="utf-8").read()
fixed_json = ruby_to_json(raw)


try:
    data = json.loads(fixed_json)
except Exception as e:
    print("JSON parsing failed!")
    print(fixed_json[:500])
    raise e


parsed = []
for row in data:
    currency, amount = parse_price(row["price"])
    parsed.append(
        {
            "id": str(row["id"]),
            "title": row["title"],
            "author": row["author"],
            "genre": row["genre"],
            "publisher": row["publisher"],
            "year": row["year"],
            "price_value": amount,
            "price_currency": currency,
        }
    )

conn = psycopg2.connect(DB)
cur = conn.cursor()

cur.execute(
    """
    DROP TABLE IF EXISTS books_raw;

    CREATE TABLE books_raw (
        id TEXT PRIMARY KEY, 
        title TEXT,
        author TEXT,
        genre TEXT,
        publisher TEXT,
        year INT,
        price_value NUMERIC,
        price_currency TEXT
    );
"""
)

for row in parsed:
    cur.execute(
        """
        INSERT INTO books_raw
        (id, title, author, genre, publisher, year, price_value, price_currency)
        VALUES (%(id)s, %(title)s, %(author)s, %(genre)s,
                %(publisher)s, %(year)s, %(price_value)s, %(price_currency)s);
        """,
        row,
    )

conn.commit()
cur.close()
conn.close()

print("Loaded books_raw successfully")
