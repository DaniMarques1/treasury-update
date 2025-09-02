import os
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from dotenv import load_dotenv
import re
from collections import defaultdict

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def parse_date(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def is_date_key(k: str) -> bool:
    return isinstance(k, str) and DATE_RE.match(k) is not None

def to_number(val) -> int | float:
    if isinstance(val, int):
        return val
    f = float(val)
    return int(f) if f.is_integer() else f

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in your environment.")

MONGO_DB = "data"
MONGO_COLLECTION = "dailyData"
TARGET_COLLECTION = "overviewData"
TOKENS_COLLECTION = "tokens"

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
daily_data_col = db[MONGO_COLLECTION]
overview_col = db[TARGET_COLLECTION]
tokens_col = db[TOKENS_COLLECTION]

token_map_doc = tokens_col.find_one({"_id": "token_symbol_map"})
if not token_map_doc or "tokens" not in token_map_doc:
    raise RuntimeError("Token symbol map not found in 'tokens' collection.")

token_map = token_map_doc["tokens"]

decimals_doc = tokens_col.find_one({"_id": "token_decimals"})
token_decimals = decimals_doc.get("decimals", {}) if decimals_doc else {}

fallback_decimals = {
    "RON": 18,
    "WETH": 18,
    "AXS": 18,
    "USDC": 6,
}

TOKEN_SYMBOLS = sorted(set(token_map.values()))

def get_decimals(token_addr: str, token_symbol: str) -> int:
    return int(
        token_decimals.get(token_addr)
        or fallback_decimals.get(token_symbol, 18)
    )

latest_overview = overview_col.find(
    {"date": {"$type": "string"}}, projection={"date": 1}
).sort("date", -1).limit(1)

latest_written_date = None
for d in latest_overview:
    latest_written_date = parse_date(d.get("date"))
    break

utc_today = datetime.now(timezone.utc).date()
end_date_inclusive = utc_today - timedelta(days=1)

print(f"Latest written date in overviewData: {latest_written_date}")
print(f"Last fully completed UTC day: {end_date_inclusive}")

if latest_written_date is not None and latest_written_date >= end_date_inclusive:
    print("No new full UTC days to write. Done.")
    raise SystemExit(0)

totals_by_date: dict[str, defaultdict[str, Decimal]] = {}

cursor = daily_data_col.find({}, no_cursor_timeout=True)
try:
    for doc in cursor:
        for key, value in doc.items():
            if key == "_id" or not is_date_key(key) or not isinstance(value, list):
                continue

            d = parse_date(key)
            if d is None:
                continue

            if latest_written_date is not None and d <= latest_written_date:
                continue
            if d > end_date_inclusive:
                continue

            if key not in totals_by_date:
                totals_by_date[key] = defaultdict(Decimal)

            day_totals = totals_by_date[key]

            for entry in value:
                token_addr = entry.get("token")
                if token_addr not in token_map:
                    continue

                sum_value_str = entry.get("_sum", {}).get("value", "0")

                try:
                    amount = Decimal(str(sum_value_str))
                except Exception:
                    amount = Decimal(0)

                token_symbol = token_map[token_addr]
                decimals = get_decimals(token_addr, token_symbol)

                if decimals > 0:
                    amount = amount / (Decimal(10) ** decimals)

                day_totals[token_symbol] += amount
finally:
    cursor.close()

if not totals_by_date:
    print("Nothing to write (no eligible dates found in dailyData within the window).")
    raise SystemExit(0)

dates_sorted = sorted(totals_by_date.keys())

written = 0
for date_str in dates_sorted:
    totals = totals_by_date[date_str]

    overview_doc = {"date": date_str}
    for symbol in TOKEN_SYMBOLS:
        overview_doc[symbol] = to_number(totals.get(symbol, Decimal(0)))

    overview_col.update_one(
        {"date": date_str},
        {"$set": overview_doc},
        upsert=True
    )
    written += 1

print(f"Wrote/updated {written} day(s) into 'overviewData' (from {dates_sorted[0]} to {dates_sorted[-1]}).")
print("Overview data aggregation completed (filtered to token list, zero-filled, int-if-whole).")
