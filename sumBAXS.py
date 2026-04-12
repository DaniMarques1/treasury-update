import os
import time
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from decimal import Decimal

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

# Filtros específicos (Normalizados para minúsculo)
TARGET_FROM = "0xb4c12d442fb0f90eba1fe5c63498aa91c02bc183".lower()
TARGET_TOKEN = "0x97a9107c1793bc407d6f527b77e7fff4D812bece".lower()

TYPE_MAP = {
    "0x8264f2c2": "Breeding",
    "0xdb116baf": "Ascend",
    "0xc7a23520": "Evolve",
    "0x8a4c44aa": "Forge",
    "0x34fcd5be": "Forge",
    "0x8aff15ec": "Runes & Charms",
    "0x26196e86": "bAXS Fee"
}


def process_and_update():
    print(f"[{datetime.now()}] 🚀 Iniciando processamento de alta performance...")
    client = MongoClient(DATABASE_URL)
    db = client['data']

    col_input = db['txns']
    col_daily = db['BAXSdaily']
    col_sum = db['BAXSsum']

    # Filtro direto no MongoDB (Utiliza índices se existirem)
    query = {
        "fromAddress": {"$regex": f"^{TARGET_FROM}$", "$options": "i"},
        "tokenAddress": {"$regex": f"^{TARGET_TOKEN}$", "$options": "i"},
        "method": {"$in": list(TYPE_MAP.keys())}
    }

    axs_daily = {}
    grand_totals = {method: Decimal(0) for method in set(TYPE_MAP.values())}

    # Buscando apenas o que precisamos
    cursor = col_input.find(query)

    count = 0
    for tx in cursor:
        count += 1
        # Converte timestamp para string de data (YYYY-MM-DD)
        dt_object = datetime.fromtimestamp(tx["timestamp"])
        date_str = dt_object.strftime("%Y-%m-%d")

        method_id = tx.get("method")
        category = TYPE_MAP[method_id]

        # Cálculo de valor
        raw_value = tx.get("amount", 0)
        try:
            # Transforma em Decimal para precisão absoluta
            val = Decimal(str(raw_value)) / Decimal(10 ** 18)

            if date_str not in axs_daily:
                axs_daily[date_str] = {m: Decimal(0) for m in set(TYPE_MAP.values())}

            axs_daily[date_str][category] += val
            grand_totals[category] += val
        except Exception as e:
            continue

    print(f"[{datetime.now()}] 📊 Transações processadas: {count}")

    if not axs_daily:
        print(f"[{datetime.now()}] ⚠️ Nenhuma transação encontrada com os filtros aplicados.")
        return

    # Bulk Upsert para BAXSdaily
    requests = [
        UpdateOne(
            {"date": d},
            {"$set": {m: float(v) for m, v in vals.items()}},
            upsert=True
        ) for d, vals in axs_daily.items()
    ]

    if requests:
        col_daily.bulk_write(requests)

    # Update Totais Globais
    sum_doc = {m: float(v) for m, v in grand_totals.items()}
    sum_doc["absolute_total"] = float(sum(grand_totals.values()))
    sum_doc["last_update"] = datetime.now()
    col_sum.replace_one({"_id": "global_totals"}, sum_doc, upsert=True)

    print(f"[{datetime.now()}] ✅ Banco de dados BAXS atualizado com sucesso!")


def run_scheduler():
    print("--- Worker BAXS (Estrutura Plana) Ativo ---")

    # Execução inicial
    try:
        process_and_update()
    except Exception as e:
        print(f"Erro: {e}")

    while True:
        now = datetime.now()
        target = now.replace(hour=23, minute=59, second=50, microsecond=0)
        if now >= target: target += timedelta(days=1)

        wait_seconds = (target - now).total_seconds()
        print(f"Aguardando próxima atualização em: {target.strftime('%d/%m/%Y %H:%M:%S')}")
        time.sleep(wait_seconds)

        try:
            process_and_update()
        except Exception as e:
            print(f"Erro na execução: {e}")
        time.sleep(10)


if __name__ == "__main__":
    run_scheduler()