import os
import time
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from decimal import Decimal

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

TYPE_MAP = {
    "0x8264f2c2": "Breeding", "0x6e094ff4": "Ascend", "0xc27b4f9b": "Ascend",
    "0xdb116baf": "Ascend", "0x2d7ab081": "Ascend", "0x2c7fa5d8": "Evolve",
    "0xc7a23520": "Evolve", "0x43afef80": "Atia's Restore", "0x8a4c44aa": "Forge",
    "0x34fcd5be": "Forge", "0x8aff15ec": "Runes & Charms", "0x95fb7103": "Runes & Charms",
    "0x26196e86": "bAXS Fee", "0xa9059cbb": "Other", "0x6a761202": "Other"
}


# Helper para garantir que pegamos sempre a hora atual em UTC
def get_now_utc():
    return datetime.now(timezone.utc)


def process_and_update():
    now_utc = get_now_utc()
    print(f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC] Iniciando processamento de dados...")

    client = MongoClient(DATABASE_URL)
    db = client['data']
    col_input, col_daily, col_sum = db['dailyData'], db['dailyAXS'], db['sumAXS']

    axs_daily = {}
    grand_totals = {method: Decimal(0) for method in set(TYPE_MAP.values())}

    # Agregação dos dados da coleção dailyData
    for doc in col_input.find():
        for key, entries in doc.items():
            if not (isinstance(key, str) and key.startswith("20")): continue
            if key not in axs_daily:
                axs_daily[key] = {method: Decimal(0) for method in set(TYPE_MAP.values())}

            for entry in entries:
                t_name = entry.get("typeName")
                if t_name in TYPE_MAP:
                    method = TYPE_MAP[t_name]
                    val = Decimal(entry["_sum"]["value"]) / Decimal(10 ** 18)
                    axs_daily[key][method] += val
                    grand_totals[method] += val

    # Bulk Upsert na dailyAXS (sobrescreve se a data já existir)
    requests = [
        UpdateOne(
            {"date": d},
            {"$set": {m: float(v) for m, v in vals.items()}},
            upsert=True
        ) for d, vals in axs_daily.items()
    ]

    if requests:
        col_daily.bulk_write(requests)

    # Replace total na sumAXS
    sum_doc = {m: float(v) for m, v in grand_totals.items()}
    sum_doc["absolute_total"] = float(sum(grand_totals.values()))
    sum_doc["last_update"] = get_now_utc()  # Salva o horário correto em UTC no banco

    col_sum.replace_one({"_id": "global_totals"}, sum_doc, upsert=True)

    print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] Banco de dados atualizado com sucesso!")


if __name__ == "__main__":
    try:
        process_and_update()
    except Exception as e:
        print(f"Erro na execução: {e}")