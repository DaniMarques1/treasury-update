import os
import json
import time  # Importado para controlar o intervalo
from datetime import datetime, timezone
from web3 import Web3
from web3.middleware import geth_poa_middleware
from pymongo import MongoClient
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Configurações de Conexão
RPC_URL = "https://api.roninchain.com/rpc"
TOKEN_ADDRESS = "0x97a9107C1793bc407d6f527b77e7fff4d812bece"
STAKING_POOL = "0x05b0bb3C1c320b280501b86706c3551995bc8571"
DATABASE_URL = os.getenv("DATABASE_URL")

# Constante de tempo (59 min e 55 seg)
INTERVALO_SEGUNDOS = (59 * 60) + 55

# Inicialização Web3
w3 = Web3(Web3.HTTPProvider(RPC_URL))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

MIN_ABI = [
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}], "name": "balanceOf",
     "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
]


def save_to_mongo(data_dict):
    """Conecta ao MongoDB e insere o documento"""
    try:
        client = MongoClient(DATABASE_URL)
        db = client['data']
        collection = db['stakingAXS']
        result = collection.insert_one(data_dict)
        client.close()  # Boa prática fechar a conexão
        return result.inserted_id
    except Exception as e:
        print(f"Erro ao salvar no MongoDB: {e}")
        return None


def coletar_dados():
    """Executa a lógica de coleta e salvamento"""
    if not w3.is_connected():
        print(json.dumps({"error": "Falha na conexão RPC"}))
        return

    contract = w3.eth.contract(address=Web3.to_checksum_address(TOKEN_ADDRESS), abi=MIN_ABI)

    try:
        now_utc = datetime.now(timezone.utc)
        decimals = contract.functions.decimals().call()
        balance_raw = contract.functions.balanceOf(Web3.to_checksum_address(STAKING_POOL)).call()
        balance_human = float(balance_raw / (10 ** decimals))

        document = {
            "date": now_utc.strftime("%Y-%m-%d"),
            "hour": now_utc.strftime("%H:%M:%S"),
            "timestamp": int(now_utc.timestamp()),
            "axs_total": balance_human
        }

        print(f"[{now_utc.strftime('%H:%M:%S')}] Dados coletados: {balance_human} AXS")
        save_to_mongo(document)

    except Exception as e:
        print(json.dumps({"error": str(e)}, indent=4))


if __name__ == "__main__":
    print(f"Iniciando monitoramento. Intervalo: {INTERVALO_SEGUNDOS} segundos.")

    while True:
        coletar_dados()

        # Aguarda o tempo definido antes da próxima execução
        time.sleep(INTERVALO_SEGUNDOS)