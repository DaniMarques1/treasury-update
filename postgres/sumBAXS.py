import os
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from dotenv import load_dotenv
import psycopg2.extras

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from db import get_cursor

load_dotenv()

TARGET_FROM = "0xb4c12d442fb0f90eba1fe5c63498aa91c02bc183".lower()
TARGET_TOKEN = "0x97a9107c1793bc407d6f527b77e7fff4d812bece".lower()

TYPE_MAP = {
    "0x8264f2c2": "Breeding",
    "0x6e094ff4": "Ascend",
    "0xc27b4f9b": "Ascend",
    "0xdb116baf": "Ascend",
    "0x2d7ab081": "Ascend",
    "0x2c7fa5d8": "Evolve",
    "0xc7a23520": "Evolve",
    "0x43afef80": "Atia's Restore",
    "0x8a4c44aa": "Forge",
    "0x34fcd5be": "Forge",
    "0x8aff15ec": "Runes & Charms",
    "0x95fb7103": "Runes & Charms",
    "0x26196e86": "bAXS Fee"
}

def get_now_utc():
    return datetime.now(timezone.utc)

def process_and_update():
    now_utc = get_now_utc()
    print(f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC] 🚀 Iniciando processamento sumBAXS (PostgreSQL)...")

    with get_cursor() as cur:
        today_midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

        # 2. Descobrir qual foi o último dia sumarizado no banco
        cur.execute('SELECT "date" FROM "BAXSdaily" ORDER BY "date" DESC LIMIT 1;')
        row = cur.fetchone()

        if row and row[0]:
            last_date_str = row[0]
            start_date = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        else:
            start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)

        if start_date >= today_midnight:
            last_str = row[0] if row else "Nenhum"
            print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] ⏭️ Nenhum dia completo novo para sumarizar. Último dia: {last_str}.")
            return

        print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] 🔍 Buscando txns de {start_date.strftime('%Y-%m-%d')} até {(today_midnight - timedelta(days=1)).strftime('%Y-%m-%d')}...")

        cur.execute("""
            SELECT "timestamp", "method", "amount"
            FROM "txns"
            WHERE LOWER("from_address") = %s
              AND LOWER("token_address") = %s
              AND "method" = ANY(%s)
              AND "timestamp" >= %s
              AND "timestamp" < %s
            ORDER BY "timestamp" ASC;
        """, (
            TARGET_FROM,
            TARGET_TOKEN,
            list(TYPE_MAP.keys()),
            int(start_date.timestamp()),
            int(today_midnight.timestamp())
        ))
        tx_rows = cur.fetchall()

        axs_daily = {}
        count = 0
        for ts, method_id, raw_amount in tx_rows:
            count += 1
            dt_object = datetime.fromtimestamp(ts, timezone.utc)
            date_str = dt_object.strftime("%Y-%m-%d")
            category = TYPE_MAP.get(method_id)
            if not category:
                continue

            try:
                val = Decimal(str(raw_amount)) / Decimal(10 ** 18)
                if date_str not in axs_daily:
                    axs_daily[date_str] = {m: Decimal(0) for m in set(TYPE_MAP.values())}
                axs_daily[date_str][category] += val
            except Exception:
                continue

        print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] 📊 Transações processadas: {count}")

        if axs_daily:
            daily_values = []
            for d, vals in axs_daily.items():
                daily_values.append((
                    d,
                    float(vals.get("Forge", 0)),
                    float(vals.get("Evolve", 0)),
                    float(vals.get("bAXS Fee", 0)),
                    float(vals.get("Breeding", 0)),
                    float(vals.get("Atia's Restore", 0)),
                    float(vals.get("Runes & Charms", 0)),
                    float(vals.get("Ascend", 0))
                ))

            psycopg2.extras.execute_values(cur, """
                INSERT INTO "BAXSdaily" (
                    "date", "Forge", "Evolve", "bAXS Fee", 
                    "Breeding", "Atia's Restore", "Runes & Charms", "Ascend"
                )
                VALUES %s
                ON CONFLICT ("date") DO UPDATE SET
                    "Forge" = EXCLUDED."Forge",
                    "Evolve" = EXCLUDED."Evolve",
                    "bAXS Fee" = EXCLUDED."bAXS Fee",
                    "Breeding" = EXCLUDED."Breeding",
                    "Atia's Restore" = EXCLUDED."Atia's Restore",
                    "Runes & Charms" = EXCLUDED."Runes & Charms",
                    "Ascend" = EXCLUDED."Ascend";
            """, daily_values, page_size=1000)
            print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] 💾 {len(daily_values)} dias salvos em BAXSdaily.")

        # Recalcular totais globais no banco via SQL
        print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] 🧮 Recalculando totais globais...")
        cur.execute("""
            SELECT 
                COALESCE(SUM("Forge"), 0),
                COALESCE(SUM("Evolve"), 0),
                COALESCE(SUM("bAXS Fee"), 0),
                COALESCE(SUM("Breeding"), 0),
                COALESCE(SUM("Atia's Restore"), 0),
                COALESCE(SUM("Runes & Charms"), 0),
                COALESCE(SUM("Ascend"), 0)
            FROM "BAXSdaily";
        """)
        totals = cur.fetchone()
        forge, evolve, baxs_fee, breeding, atia, runes, ascend = totals
        abs_total = float(forge + evolve + baxs_fee + breeding + atia + runes + ascend)

        cur.execute("""
            INSERT INTO "BAXSsum" (
                "id", "Forge", "Evolve", "bAXS Fee", "Breeding", 
                "Atia's Restore", "Runes & Charms", "Ascend", "absolute_total", "last_update"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("id") DO UPDATE SET
                "Forge" = EXCLUDED."Forge",
                "Evolve" = EXCLUDED."Evolve",
                "bAXS Fee" = EXCLUDED."bAXS Fee",
                "Breeding" = EXCLUDED."Breeding",
                "Atia's Restore" = EXCLUDED."Atia's Restore",
                "Runes & Charms" = EXCLUDED."Runes & Charms",
                "Ascend" = EXCLUDED."Ascend",
                "absolute_total" = EXCLUDED."absolute_total",
                "last_update" = EXCLUDED."last_update";
        """, (
            "global_totals",
            float(forge), float(evolve), float(baxs_fee),
            float(breeding), float(atia), float(runes), float(ascend),
            abs_total,
            get_now_utc()
        ))

    print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] ✅ Banco de dados BAXS atualizado com sucesso!")

if __name__ == "__main__":
    try:
        process_and_update()
    except Exception as e:
        print(f"Erro na execução de sumBAXS: {e}")
