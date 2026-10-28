import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from dotenv import load_dotenv
import psycopg2.extras

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from db import get_cursor

load_dotenv()

TYPE_MAP = {
    "0x8264f2c2": "Breeding", "0x6e094ff4": "Ascend", "0xc27b4f9b": "Ascend",
    "0xdb116baf": "Ascend", "0x2d7ab081": "Ascend", "0x2c7fa5d8": "Evolve",
    "0xc7a23520": "Evolve", "0x43afef80": "Atia's Restore", "0x8a4c44aa": "Forge",
    "0x34fcd5be": "Forge", "0x8aff15ec": "Runes & Charms", "0x95fb7103": "Runes & Charms",
    "0x26196e86": "bAXS Fee", "0xa9059cbb": "Other", "0x6a761202": "Other"
}

def get_now_utc():
    return datetime.now(timezone.utc)

def process_and_update():
    now_utc = get_now_utc()
    print(f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC] Iniciando processamento de sumAXS (PostgreSQL)...")

    with get_cursor() as cur:
        # Busca todas as entradas da tabela dailyData
        cur.execute('SELECT "date", "entries" FROM "dailyData" ORDER BY "date" ASC;')
        rows = cur.fetchall()

        axs_daily = {}
        grand_totals = {method: Decimal(0) for method in set(TYPE_MAP.values())}

        for date_str, entries in rows:
            if not (isinstance(date_str, str) and date_str.startswith("20")):
                continue
            if date_str not in axs_daily:
                axs_daily[date_str] = {method: Decimal(0) for method in set(TYPE_MAP.values())}

            if not isinstance(entries, list):
                continue

            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                t_name = entry.get("typeName")
                if t_name in TYPE_MAP:
                    method = TYPE_MAP[t_name]
                    sum_obj = entry.get("_sum", {})
                    raw_val = sum_obj.get("value")
                    if raw_val is not None:
                        try:
                            val = Decimal(str(raw_val)) / Decimal(10 ** 18)
                            axs_daily[date_str][method] += val
                            grand_totals[method] += val
                        except Exception:
                            continue

        # Bulk Upsert na tabela dailyAXS
        if axs_daily:
            daily_values = []
            for d, vals in axs_daily.items():
                daily_values.append((
                    d,
                    float(vals.get("Forge", 0)),
                    float(vals.get("Evolve", 0)),
                    float(vals.get("Other", 0)),
                    float(vals.get("bAXS Fee", 0)),
                    float(vals.get("Breeding", 0)),
                    float(vals.get("Atia's Restore", 0)),
                    float(vals.get("Runes & Charms", 0)),
                    float(vals.get("Ascend", 0))
                ))

            psycopg2.extras.execute_values(cur, """
                INSERT INTO "dailyAXS" (
                    "date", "Forge", "Evolve", "Other", "bAXS Fee", 
                    "Breeding", "Atia's Restore", "Runes & Charms", "Ascend"
                )
                VALUES %s
                ON CONFLICT ("date") DO UPDATE SET
                    "Forge" = EXCLUDED."Forge",
                    "Evolve" = EXCLUDED."Evolve",
                    "Other" = EXCLUDED."Other",
                    "bAXS Fee" = EXCLUDED."bAXS Fee",
                    "Breeding" = EXCLUDED."Breeding",
                    "Atia's Restore" = EXCLUDED."Atia's Restore",
                    "Runes & Charms" = EXCLUDED."Runes & Charms",
                    "Ascend" = EXCLUDED."Ascend";
            """, daily_values, page_size=1000)
            print(f"[{get_now_utc().strftime('%H:%M:%S')} UTC] 💾 {len(daily_values)} dias atualizados em dailyAXS.")

        # Upsert total na sumAXS
        abs_total = float(sum(grand_totals.values()))
        cur.execute("""
            INSERT INTO "sumAXS" (
                "id", "Forge", "Evolve", "Other", "bAXS Fee", "Breeding", 
                "Atia's Restore", "Runes & Charms", "Ascend", "absolute_total", "last_update"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("id") DO UPDATE SET
                "Forge" = EXCLUDED."Forge",
                "Evolve" = EXCLUDED."Evolve",
                "Other" = EXCLUDED."Other",
                "bAXS Fee" = EXCLUDED."bAXS Fee",
                "Breeding" = EXCLUDED."Breeding",
                "Atia's Restore" = EXCLUDED."Atia's Restore",
                "Runes & Charms" = EXCLUDED."Runes & Charms",
                "Ascend" = EXCLUDED."Ascend",
                "absolute_total" = EXCLUDED."absolute_total",
                "last_update" = EXCLUDED."last_update";
        """, (
            "global_totals",
            float(grand_totals.get("Forge", 0)),
            float(grand_totals.get("Evolve", 0)),
            float(grand_totals.get("Other", 0)),
            float(grand_totals.get("bAXS Fee", 0)),
            float(grand_totals.get("Breeding", 0)),
            float(grand_totals.get("Atia's Restore", 0)),
            float(grand_totals.get("Runes & Charms", 0)),
            float(grand_totals.get("Ascend", 0)),
            abs_total,
            get_now_utc()
        ))

    print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] Banco de dados sumAXS atualizado com sucesso!")

if __name__ == "__main__":
    try:
        process_and_update()
    except Exception as e:
        print(f"Erro na execução de sumAXS: {e}")
