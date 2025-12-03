import pandas as pd
import numpy as np
import re
import yaml
import matplotlib.pyplot as plt
import json
from dateutil import parser, tz


def clean_price(value):
    if pd.isna(value):
        return np.nan
    s = str(value)
    s = s.replace(" ", "").replace(",", ".")
    s = s.replace("USD", "").replace("EUR", "")
    s = s.replace("$", "").replace("€", "")

    s = re.sub(r"¢(\d+)", r".\1", s)
    s = s.replace("¢", ".")

    s = re.sub(r"(\d+)\$(\d+)", r"\1.\2", s)

    match = re.search(r"\d+(\.\d+)?", s)
    return float(match.group()) if match else np.nan


def normalize_ts(s):
    if pd.isna(s):
        return None
    s = str(s).strip()

    s = (
        s.replace("A.M.", "AM")
        .replace("P.M.", "PM")
        .replace("a.m.", "AM")
        .replace("p.m.", "PM")
        .replace("am", "AM")
        .replace("pm", "PM")
    )
    s = s.replace(";", " ").replace(",", " ")
    return s


def parse_ts(s):
    if pd.isna(s):
        return pd.NaT
    s = normalize_ts(s)

    fmts = [
        "%m/%d/%y %I:%M:%S %p",
        "%I:%M:%S %p %Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
    ]

    for fmt in fmts:
        try:
            return pd.to_datetime(s, format=fmt, utc=True)
        except:
            pass

    try:
        dt = parser.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz.UTC)
        else:
            dt = dt.astimezone(tz.UTC)
        return pd.Timestamp(dt)
    except:
        return pd.NaT


orders = pd.read_parquet("data/DATA3/orders.parquet")
users = pd.read_csv("data/DATA3/users.csv")

with open("data/DATA3/books.yaml", "r") as f:
    raw_yaml = yaml.safe_load(f)


books = []
for item in raw_yaml:
    fixed = {k.replace(":", ""): v for k, v in item.items()}
    books.append(fixed)

books = pd.DataFrame(books)


orders["unit_price_clean"] = orders["unit_price"].apply(clean_price)
orders["timestamp_clean"] = orders["timestamp"].apply(parse_ts)
orders["timestamp_clean"] = pd.to_datetime(orders["timestamp_clean"], utc=True)

orders["date"] = orders["timestamp_clean"].dt.date
orders["year"] = orders["timestamp_clean"].dt.year
orders["month"] = orders["timestamp_clean"].dt.month
orders["day"] = orders["timestamp_clean"].dt.day

orders["shipping"] = orders["shipping"].replace(
    {"NULL": np.nan, "null": np.nan, "": np.nan}
)


def currency_factor(x):
    s = str(x).lower()
    if "€" in s or "eur" in s:
        return 1.2
    return 1.0


orders["currency_factor"] = orders["unit_price"].apply(currency_factor)
orders["unit_price_usd"] = orders["unit_price_clean"] * orders["currency_factor"]
orders["paid_price"] = orders["quantity"] * orders["unit_price_usd"]


for col in ["email", "phone", "address", "name"]:
    users[col] = users[col].replace({"NULL": np.nan, "null": np.nan, "": np.nan})


def cluster_users(df):
    groups = []
    used = set()

    for i, r in df.iterrows():
        if i in used:
            continue
        group = {r["id"]}
        used.add(i)

        for j, r2 in df.iloc[i + 1 :].iterrows():
            if j in used:
                continue

            matches = (
                (r["email"] == r2["email"] and pd.notna(r["email"]))
                + (r["phone"] == r2["phone"] and pd.notna(r["phone"]))
                + (r["address"] == r2["address"] and pd.notna(r["address"]))
                + (r["name"] == r2["name"] and pd.notna(r["name"]))
            )
            if matches >= 3:
                group.add(r2["id"])
                used.add(j)

        groups.append(group)
    return groups


user_clusters = cluster_users(users)
unique_user_count = len(user_clusters)


def normalize_author_string(a):
    if pd.isna(a):
        return ("Unknown",)

    parts = [p.strip() for p in a.split(",") if p.strip()]
    clean_parts = []

    for p in parts:
        p = re.sub(r"\b(Rep\.|Sen\.|LLD|PhD|Dr\.)\b", "", p).strip()
        p = " ".join(p.split())

        # normalize case
        clean_parts.append(p.title())

    return tuple(sorted(clean_parts))


books["author_set"] = books["author"].apply(normalize_author_string)


daily_rev = orders.groupby("date")["paid_price"].sum().reset_index()
top5_days = daily_rev.sort_values("paid_price", ascending=False).head(5)

unique_users = unique_user_count

unique_author_sets = books["author_set"].nunique()

merged = orders.merge(books, left_on="book_id", right_on="id")
author_sales = (
    merged.groupby("author_set")["quantity"].sum().sort_values(ascending=False)
)
best_author = author_sales.index[0]

user_spend = orders.groupby("user_id")["paid_price"].sum().sort_values(ascending=False)
top_user_id = user_spend.index[0]

top_cluster = [list(c) for c in user_clusters if top_user_id in c][0]


plt.figure(figsize=(10, 4))
plt.plot(daily_rev["date"], daily_rev["paid_price"])
plt.title("Daily Revenue")
plt.xlabel("Date")
plt.ylabel("Revenue ($)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("DATA3_daily_revenue.png")


output = {
    "top_5_days": top5_days["date"].astype(str).tolist(),
    "unique_users": unique_users,
    "unique_author_sets": int(unique_author_sets),
    "best_author": list(best_author),
    "top_buyer_cluster": top_cluster,
}

with open("DATA3_summary.json", "w") as f:
    json.dump(output, f, indent=4)

print("✔ DATA processed successfully!")
print(json.dumps(output, indent=4))
