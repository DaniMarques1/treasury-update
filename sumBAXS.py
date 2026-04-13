import os
import time
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from decimal import Decimal

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

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


# Helper para garantir que pegamos sempre a hora atual em UTC
def get_now_utc():
    return datetime.now(timezone.utc)


def process_and_update():
    now_utc = get_now_utc()
    print(f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC] 🚀 Iniciando processamento de alta performance...")

    client = MongoClient(DATABASE_URL)
    db = client['data']

    col_input = db['txns']
    col_daily = db['BAXSdaily']
    col_sum = db['BAXSsum']

    # 1. Identificar o limite de tempo (Hoje às 00:00:00 UTC)
    today_midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    # 2. Descobrir qual foi o último dia sumarizado no banco
    last_summary = col_daily.find_one(sort=[("date", -1)])

    if last_summary and "date" in last_summary:
        last_date_str = last_summary["date"]
        # Início da busca: Adiciona 1 dia ao último salvo e atrela ao timezone UTC
        start_date = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    else:
        # Fallback: Data inicial setada como UTC
        start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)

    # Se o dia de início calculado for hoje ou no futuro, não há dias completos pendentes
    if start_date >= today_midnight:
        print(
            f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] ⏭️ Nenhum dia completo novo para sumarizar. Último dia processado: {last_summary.get('date')}.")
        return

    print(
        f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] 🔍 Buscando txns de {start_date.strftime('%Y-%m-%d')} até {(today_midnight - timedelta(days=1)).strftime('%Y-%m-%d')}...")

    # 3. Query com filtro de timestamp
    query = {
        "fromAddress": {"$regex": f"^{TARGET_FROM}$", "$options": "i"},
        "tokenAddress": {"$regex": f"^{TARGET_TOKEN}$", "$options": "i"},
        "method": {"$in": list(TYPE_MAP.keys())},
        "timestamp": {
            "$gte": start_date.timestamp(),  # .timestamp() gera o Epoch universal correto
            "$lt": today_midnight.timestamp()
        }
    }

    axs_daily = {}
    cursor = col_input.find(query)
    count = 0

    for tx in cursor:
        count += 1
        # Converte o epoch em datetime garantindo a leitura em UTC
        dt_object = datetime.fromtimestamp(tx["timestamp"], timezone.utc)
        date_str = dt_object.strftime("%Y-%m-%d")

        method_id = tx.get("method")
        category = TYPE_MAP[method_id]
        raw_value = tx.get("amount", 0)

        try:
            val = Decimal(str(raw_value)) / Decimal(10 ** 18)

            if date_str not in axs_daily:
                axs_daily[date_str] = {m: Decimal(0) for m in set(TYPE_MAP.values())}

            axs_daily[date_str][category] += val
        except Exception as e:
            continue

    print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] 📊 Transações processadas nesta rodada: {count}")

    if axs_daily:
        # Bulk Upsert para BAXSdaily
        requests = [
            UpdateOne(
                {"date": d},
                {"$set": {m: float(v) for m, v in vals.items()}},
                upsert=True
            ) for d, vals in axs_daily.items()
        ]
        col_daily.bulk_write(requests)
        print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] 💾 {len(requests)} dias salvos em BAXSdaily.")

    # 4. Atualizar Totais Globais (Agregação no banco)
    print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] 🧮 Recalculando totais globais...")

    unique_methods = set(TYPE_MAP.values())
    group_stage = {"_id": "global_totals"}
    for method in unique_methods:
        group_stage[method] = {"$sum": f"${method}"}

    pipeline = [{"$group": group_stage}]
    aggr_result = list(col_daily.aggregate(pipeline))

    if aggr_result:
        sum_doc = aggr_result[0]
        sum_doc["absolute_total"] = sum(sum_doc.get(m, 0) for m in unique_methods)
        sum_doc["last_update"] = get_now_utc()  # Registra o update global com timezone explícito

        col_sum.replace_one({"_id": "global_totals"}, sum_doc, upsert=True)

    print(f"[{get_now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] ✅ Banco de dados BAXS atualizado com sucesso!")


if __name__ == "__main__":
    try:
        process_and_update()
    except Exception as e:
        print(f"Erro na execução: {e}")
