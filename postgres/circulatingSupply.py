import os
import sys
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from db import get_cursor

load_dotenv()

def update_circulating_supply():
    api_url = "https://skynet-api.roninchain.com/ronin/supplies/axs/circulating"

    try:
        # 1. Requisição dos dados
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()

        supply_value = float(data["result"]["value"])

        # 2. Captura o tempo atual em UTC 0
        now_utc = datetime.now(timezone.utc)

        # 3. Montagem do documento
        document = {
            "circulating_supply": supply_value,
            "datetime": now_utc.strftime("%Y/%m/%d %H:%M"),
            "timestamp": int(now_utc.timestamp())
        }

        # 4. Inserção no PostgreSQL
        with get_cursor() as cur:
            cur.execute("""
                INSERT INTO "circulatingSupply" ("circulating_supply", "datetime", "timestamp")
                VALUES (%s, %s, %s)
                RETURNING "id";
            """, (
                document["circulating_supply"],
                document["datetime"],
                document["timestamp"]
            ))
            inserted_id = cur.fetchone()[0]

        print(f"✅ Sucesso (UTC 0)! Registro inserido no PostgreSQL (ID: {inserted_id})")
        print(f"Dados salvos: {document}")

    except Exception as e:
        print(f"❌ Erro na operação circulatingSupply: {e}")

if __name__ == "__main__":
    while True:
        update_circulating_supply()
        print("⏳ Aguardando 1 hora para próxima execução...")
        time.sleep(3600)
