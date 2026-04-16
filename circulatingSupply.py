import os
import requests
from pymongo import MongoClient
from datetime import datetime, timezone
from dotenv import load_dotenv
import time

# Carrega as variáveis do arquivo .env
load_dotenv()


def update_circulating_supply():
    api_url = "https://skynet-api.roninchain.com/ronin/supplies/axs/circulating"
    mongo_uri = os.getenv("DATABASE_URL")

    if not mongo_uri:
        print("Erro: DATABASE_URL não encontrada no arquivo .env")
        return

    try:
        # 1. Requisição dos dados
        response = requests.get(api_url)
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

        # 4. Inserção no MongoDB
        client = MongoClient(mongo_uri)
        db = client["data"]
        collection = db["circulatingSupply"]

        result = collection.insert_one(document)
        print(f"✅ Sucesso (UTC 0)! Registro inserido.")
        print(f"Dados salvos: {document}")

    except Exception as e:
        print(f"❌ Erro na operação: {e}")
    finally:
        if 'client' in locals():
            client.close()


if __name__ == "__main__":
    while True:
        update_circulating_supply()
        print("⏳ Aguardando 1 hora para próxima execução...")
        time.sleep(3600)