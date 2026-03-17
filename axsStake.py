import os
import json
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
        # Acessa o banco 'data' e a coleção 'stakingAXS'
        db = client['data']
        collection = db['stakingAXS']

        result = collection.insert_one(data_dict)
        return result.inserted_id
    except Exception as e:
        print(f"Erro ao salvar no MongoDB: {e}")
        return None


def main():
    if not w3.is_connected():
        print(json.dumps({"error": "Falha na conexão RPC"}))
        return

    contract = w3.eth.contract(address=Web3.to_checksum_address(TOKEN_ADDRESS), abi=MIN_ABI)

    try:
        now_utc = datetime.now(timezone.utc)

        # Consulta de dados no Blockchain
        decimals = contract.functions.decimals().call()
        balance_raw = contract.functions.balanceOf(Web3.to_checksum_address(STAKING_POOL)).call()
        balance_human = float(balance_raw / (10 ** decimals))

        # Estrutura do Documento
        document = {
            "date": now_utc.strftime("%Y-%m-%d"),
            "hour": now_utc.strftime("%H:%M:%S"),
            "timestamp": int(now_utc.timestamp()),
            "axs_total": balance_human
        }

        # 1. Print do JSON (para seu log/terminal)
        print(json.dumps(document, indent=4))

        # 2. Salva no MongoDB
        inserted_id = save_to_mongo(document.copy())  # .copy() para evitar problemas de mutação

        if inserted_id:
            # Opcional: print discreto de sucesso
            # print(f"Inserido com ID: {inserted_id}")
            pass

    except Exception as e:
        print(json.dumps({"error": str(e)}, indent=4))


if __name__ == "__main__":
    main()