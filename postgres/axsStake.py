import os
import sys
import json
import time
from datetime import datetime, timezone
from web3 import Web3
from web3.middleware import geth_poa_middleware
from dotenv import load_dotenv

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from db import get_cursor

load_dotenv()

RPC_URL = "https://api.roninchain.com/rpc"
TOKEN_ADDRESS = "0x97a9107C1793bc407d6f527b77e7fff4d812bece"
STAKING_POOL = "0x05b0bb3C1c320b280501b86706c3551995bc8571"

# Constante de tempo (59 min e 55 seg)
INTERVALO_SEGUNDOS = (59 * 60) + 55

w3 = Web3(Web3.HTTPProvider(RPC_URL))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

MIN_ABI = [
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}], "name": "balanceOf",
     "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
]

def save_to_postgres(document):
    """Conecta ao PostgreSQL e insere o registro na tabela stakingAXS"""
    try:
        with get_cursor() as cur:
            cur.execute("""
                INSERT INTO "stakingAXS" ("date", "hour", "timestamp", "axs_total")
                VALUES (%s, %s, %s, %s)
                RETURNING "id";
            """, (
                document["date"],
                document["hour"],
                document["timestamp"],
                document["axs_total"]
            ))
            inserted_id = cur.fetchone()[0]
            return inserted_id
    except Exception as e:
        print(f"Erro ao salvar no PostgreSQL: {e}")
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
        res_id = save_to_postgres(document)
        if res_id:
            print(f"✅ Registro salvo no PostgreSQL (ID: {res_id})")

    except Exception as e:
        print(json.dumps({"error": str(e)}, indent=4))

if __name__ == "__main__":
    print(f"Iniciando monitoramento stakingAXS (PostgreSQL). Intervalo: {INTERVALO_SEGUNDOS} segundos.")
    while True:
        coletar_dados()
        time.sleep(INTERVALO_SEGUNDOS)
