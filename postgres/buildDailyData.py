import os
import sys
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import psycopg2.extras

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from db import get_cursor

load_dotenv()

DEFAULT_START_DATE = datetime(2025, 7, 27, tzinfo=timezone.utc).date()

def parse_yyyy_mm_dd(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def find_resume_start_date(cur):
    cur.execute('SELECT "date", "entries" FROM "dailyData";')
    rows = cur.fetchall()

    valid_dates = set()
    empty_or_invalid_dates = set()

    for date_str, entries in rows:
        d = parse_yyyy_mm_dd(date_str)
        if d:
            if isinstance(entries, list) and len(entries) > 0:
                valid_dates.add(d)
            else:
                empty_or_invalid_dates.add(d)

    if empty_or_invalid_dates:
        earliest_empty = min(empty_or_invalid_dates)
        print(f"🔄 Encontrados dias vazios no banco. Recomeçando a partir de: {earliest_empty}")
        return earliest_empty

    if valid_dates:
        return max(valid_dates) + timedelta(days=1)

    return DEFAULT_START_DATE

def day_timestamps_utc(day: datetime.date):
    start_dt = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end_dt = start_dt.replace(hour=23, minute=59, second=59)
    return int(start_dt.timestamp()), int(end_dt.timestamp())

def aggregate_for_day(cur, start_ts: int, end_ts: int):
    cur.execute("""
        SELECT 
            "method" AS type_name,
            LOWER("token_address") AS token,
            SUM("amount_raw")::text AS total
        FROM "txns"
        WHERE "timestamp" >= %s AND "timestamp" <= %s
        GROUP BY "method", LOWER("token_address")
        ORDER BY "method" ASC, LOWER("token_address") ASC;
    """, (start_ts, end_ts))
    
    rows = cur.fetchall()
    results = []
    for type_name, token, total in rows:
        results.append({
            "_sum": {"value": str(total)},
            "typeName": type_name,
            "token": token
        })
    return results

def upsert_day(cur, date_str: str, results: list):
    cur.execute("""
        INSERT INTO "dailyData" ("date", "entries")
        VALUES (%s, %s)
        ON CONFLICT ("date") DO UPDATE SET
            "entries" = EXCLUDED."entries";
    """, (date_str, psycopg2.extras.Json(results)))

def main():
    print("Iniciando buildDailyData (PostgreSQL)...")
    with get_cursor() as cur:
        start_date = find_resume_start_date(cur)
        yesterday_utc = datetime.now(timezone.utc).date() - timedelta(days=1)

        if start_date > yesterday_utc:
            print("Nothing to do. Already up to date and all days have valid data.")
            return

        total_days = (yesterday_utc - start_date).days + 1
        print(f"Processing {total_days} day(s), from {start_date} to {yesterday_utc} (UTC).")

        current = start_date
        processed = 0
        while current <= yesterday_utc:
            date_str = current.strftime("%Y-%m-%d")
            start_ts, end_ts = day_timestamps_utc(current)
            results = aggregate_for_day(cur, start_ts, end_ts)
            upsert_day(cur, date_str, results)

            processed += 1
            if processed % 10 == 0 or current == yesterday_utc:
                remaining = total_days - processed
                print(f"--- Processed {processed}/{total_days}. Remaining: {remaining} ---")

            current += timedelta(days=1)

    print("✅ buildDailyData concluído com sucesso!")

if __name__ == "__main__":
    main()
