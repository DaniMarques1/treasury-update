import os
import sys
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from dotenv import load_dotenv
import psycopg2.extras

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from db import get_cursor

load_dotenv()

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

fallback_decimals = {
    "RON": 18,
    "WETH": 18,
    "AXS": 18,
    "USDC": 6,
}

def get_decimals(token_addr: str, token_symbol: str, token_decimals: dict) -> int:
    return int(
        token_decimals.get(token_addr)
        or fallback_decimals.get(token_symbol, 18)
    )

def is_doc_all_zeros(doc: dict, token_symbols: list) -> bool:
    for symbol in token_symbols:
        val = doc.get(symbol, 0)
        if val > 0:
            return False
    return True

def find_overview_resume_start_date(cur, token_symbols: list):
    cur.execute('SELECT "date", "AXS", "RON", "USDC", "WETH" FROM "overviewData";')
    rows = cur.fetchall()

    valid_dates = set()
    zero_dates = set()

    for date_str, axs, ron, usdc, weth in rows:
        d = parse_date(date_str)
        if not d:
            continue

        doc = {"AXS": axs, "RON": ron, "USDC": usdc, "WETH": weth}
        if is_doc_all_zeros(doc, token_symbols):
            zero_dates.add(d)
        else:
            valid_dates.add(d)

    if zero_dates:
        earliest_zero = min(zero_dates)
        print(f"🔄 Encontrados registros zerados em 'overviewData'. Recomeçando de: {earliest_zero}")
        return earliest_zero

    if valid_dates:
        return max(valid_dates) + timedelta(days=1)

    return None

def main():
    with get_cursor() as cur:
        # Carrega mapa de tokens
        cur.execute('SELECT "data" FROM "tokens" WHERE "id" = %s LIMIT 1;', ("token_symbol_map",))
        row = cur.fetchone()
        if not row or not row[0]:
            raise RuntimeError("Token symbol map não encontrado na tabela 'tokens'.")
        token_map = row[0]

        # Carrega decimais se existirem
        cur.execute('SELECT "data" FROM "tokens" WHERE "id" = %s LIMIT 1;', ("token_decimals",))
        row_dec = cur.fetchone()
        token_decimals = row_dec[0].get("decimals", {}) if row_dec and row_dec[0] else {}

        token_symbols = sorted(set(token_map.values()))

        utc_today = datetime.now(timezone.utc).date()
        end_date_inclusive = utc_today - timedelta(days=1)
        resume_start_date = find_overview_resume_start_date(cur, token_symbols)

        print(f"Resume start date for overviewData: {resume_start_date}")
        print(f"Last fully completed UTC day: {end_date_inclusive}")

        if resume_start_date is not None and resume_start_date > end_date_inclusive:
            print("No new full UTC days to write and no zeroed days found. Done.")
            return

        # Query dailyData
        cur.execute('SELECT "date", "entries" FROM "dailyData" ORDER BY "date" ASC;')
        daily_rows = cur.fetchall()

        totals_by_date = {}

        for date_str, entries in daily_rows:
            d = parse_date(date_str)
            if d is None:
                continue

            if resume_start_date is not None and d < resume_start_date:
                continue
            if d > end_date_inclusive:
                continue
            if not isinstance(entries, list) or len(entries) == 0:
                continue

            if date_str not in totals_by_date:
                totals_by_date[date_str] = defaultdict(Decimal)

            day_totals = totals_by_date[date_str]

            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                token_addr = entry.get("token")
                if token_addr not in token_map:
                    continue

                sum_value_str = entry.get("_sum", {}).get("value", "0")
                try:
                    amount = Decimal(str(sum_value_str))
                except Exception:
                    amount = Decimal(0)

                token_symbol = token_map[token_addr]
                decimals = get_decimals(token_addr, token_symbol, token_decimals)

                if decimals > 0:
                    amount = amount / (Decimal(10) ** decimals)

                day_totals[token_symbol] += amount

        if not totals_by_date:
            print("Nothing to write (no eligible dates with data found in dailyData within the window).")
            return

        dates_sorted = sorted(totals_by_date.keys())
        values_to_upsert = []

        for date_str in dates_sorted:
            totals = totals_by_date[date_str]
            values_to_upsert.append((
                date_str,
                float(totals.get("AXS", Decimal(0))),
                float(totals.get("RON", Decimal(0))),
                float(totals.get("USDC", Decimal(0))),
                float(totals.get("WETH", Decimal(0)))
            ))

        psycopg2.extras.execute_values(cur, """
            INSERT INTO "overviewData" ("date", "AXS", "RON", "USDC", "WETH")
            VALUES %s
            ON CONFLICT ("date") DO UPDATE SET
                "AXS" = EXCLUDED."AXS",
                "RON" = EXCLUDED."RON",
                "USDC" = EXCLUDED."USDC",
                "WETH" = EXCLUDED."WETH";
        """, values_to_upsert, page_size=1000)

        print(f"Wrote/updated {len(values_to_upsert)} day(s) into 'overviewData' (from {dates_sorted[0]} to {dates_sorted[-1]}).")
        print("Overview data aggregation completed.")

if __name__ == "__main__":
    main()
