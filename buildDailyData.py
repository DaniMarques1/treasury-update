import os
import json
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in your environment.")
MONGO_DB = "data"
MONGO_COLLECTION = "txns"
TARGET_COLLECTION = "dailyData"

client = MongoClient(MONGO_URI)
collection = client[MONGO_DB][MONGO_COLLECTION]
target_collection = client[MONGO_DB][TARGET_COLLECTION]

DEFAULT_START_DATE = datetime(2025, 7, 27, tzinfo=timezone.utc).date()  # 2025-07-27 (UTC)

def drop_date_index_if_exists():
    try:
        for idx in target_collection.list_indexes():
            name = idx.get("name")
            key = idx.get("key")
            if name == "date_1" or (key and list(key.keys()) == ["date"]):
                target_collection.drop_index(name)
                print("🧹 Dropped index:", name)
    except Exception as e:
        print("Note: could not inspect/drop date index:", e)

def parse_yyyy_mm_dd(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def find_resume_start_date():
    cursor = target_collection.find({}, {"_id": 0}).limit(5000)
    max_date = None
    for doc in cursor:
        for k in doc.keys():
            d = parse_yyyy_mm_dd(k)
            if d and (max_date is None or d > max_date):
                max_date = d
    if max_date:
        return max_date + timedelta(days=1)
    return DEFAULT_START_DATE

def day_timestamps_utc(day: datetime.date):
    start_dt = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end_dt = start_dt.replace(hour=23, minute=59, second=59)
    return int(start_dt.timestamp()), int(end_dt.timestamp())

def aggregate_for_day(start_ts: int, end_ts: int):
    pipeline = [
        {"$match": {"timestamp": {"$gte": start_ts, "$lte": end_ts}}},
        {
            "$group": {
                "_id": {
                    "method": "$method",
                    "token": {
                        "$toLower": {
                            "$ifNull": ["$tokenAddress", ""]
                        }
                    }
                },
                "total": {"$sum": {"$toDecimal": "$amountRaw"}}
            }
        },
        {
            "$project": {
                "_id": 0,
                "_sum": {"value": {"$toString": "$total"}},
                "typeName": "$_id.method",
                "token": "$_id.token"
            }
        },
        {"$sort": {"typeName": 1, "token": 1}}
    ]
    return list(collection.aggregate(pipeline))


def upsert_day(date_str: str, results: list):
    try:
        res = target_collection.update_one(
            {date_str: {"$exists": True}},
            {"$setOnInsert": {date_str: results}},
            upsert=True
        )
        if res.upserted_id:
            print(f"✅ Inserted day {date_str} with _id: {res.upserted_id}")
        else:
            print(f"↪️  Skipped {date_str}: already present.")
    except Exception as e:
        print(f"⚠️  Upsert error for {date_str}: {e}")

def main():
    drop_date_index_if_exists()

    start_date = find_resume_start_date()
    yesterday_utc = (datetime.now(timezone.utc).date() - timedelta(days=1))

    if start_date > yesterday_utc:
        print("Nothing to do. Already up to date.")
        return

    total_days = (yesterday_utc - start_date).days + 1
    print(f"Processing {total_days} day(s), from {start_date} to {yesterday_utc} (UTC).")

    current = start_date
    processed = 0
    while current <= yesterday_utc:
        date_str = current.strftime("%Y-%m-%d")
        start_ts, end_ts = day_timestamps_utc(current)
        results = aggregate_for_day(start_ts, end_ts)
        upsert_day(date_str, results)

        try:
            sample_out = {
                "_id": "(hidden)",
                date_str: results
            }
            print(json.dumps(sample_out, indent=2))
        except Exception:
            pass

        processed += 1
        if processed % 10 == 0 or current == yesterday_utc:
            remaining = total_days - processed
            print(f"--- Processed {processed}/{total_days}. Remaining: {remaining} ---")

        current += timedelta(days=1)

if __name__ == "__main__":
    main()
