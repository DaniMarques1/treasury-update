import os
import sys
import time
from decimal import Decimal
import requests
from web3 import Web3
from web3.middleware import geth_poa_middleware
from dotenv import load_dotenv
import psycopg2.extras

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from db import get_cursor

load_dotenv()

RONIN_RPC = "https://api.roninchain.com/rpc"

BATCH_SIZE = 100
CONFIRMATIONS = 1
POLL_INTERVAL = 20
RETRY_DELAY = 10
CYCLE_DELAY = 60
REQ_DELAY = 0.65

TREASURY_ADDRESSES = [
    Web3.to_checksum_address("0x245db945c485b68fDc429e4F7085a1761Aa4d45d"),
]
TREASURY_SET = set(TREASURY_ADDRESSES)

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
NATIVE_RON_TRANSFER_TOPIC = "0x3d0ce9bfc3ed7d6862dbb28b2dea94561fe714a1b4d019aa8af39730d1ad7c3d"

LATEST_RETRY_AFTER = 0

def response_hook(response, *args, **kwargs):
    global LATEST_RETRY_AFTER
    if not response.ok:
        print("\n" + "=" * 60)
        print(f"❌ HTTP Error: {response.status_code} {response.reason}")
        print(f"URL: {response.url}")
        if response.status_code == 429:
            retry_val = response.headers.get("retry-after") or response.headers.get("ratelimit-reset")
            try:
                LATEST_RETRY_AFTER = float(retry_val) if retry_val else 15.0
            except ValueError:
                LATEST_RETRY_AFTER = 15.0

def safe_selector(input_data) -> str:
    if hasattr(input_data, "hex"):
        input_data = input_data.hex()
    if not input_data or not isinstance(input_data, str):
        return "0x"
    return (
        input_data[:10]
        if input_data.startswith("0x") and len(input_data) >= 10
        else "0x"
    )

def with_retries(fn, *args, retries=3, delay=5, **kwargs):
    global LATEST_RETRY_AFTER
    for attempt in range(1, retries + 1):
        try:
            if REQ_DELAY > 0:
                time.sleep(REQ_DELAY)
            result = fn(*args, **kwargs)
            LATEST_RETRY_AFTER = 0
            return result
        except Exception as e:
            wait_time = LATEST_RETRY_AFTER if LATEST_RETRY_AFTER > 0 else delay
            print(f"⚠️ Tentativa {attempt}/{retries} falhou para {fn.__name__}: {e}")
            print(f"⏳ Aguardando {wait_time:.1f}s antes de tentar novamente...")
            time.sleep(wait_time)
            LATEST_RETRY_AFTER = 0
            if attempt == retries:
                raise

def get_latest_block_from_postgres():
    with get_cursor() as cur:
        cur.execute('SELECT MAX("block_number") FROM "txns";')
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    return None

def process_and_store_transactions():
    session = requests.Session()
    session.hooks["response"].append(response_hook)

    w3 = Web3(Web3.HTTPProvider(RONIN_RPC, session=session))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    if not w3.is_connected():
        raise Exception(f"❌ Failed to connect to Ronin node at {RONIN_RPC}")
    print(f"✅ Connected to Ronin. Head block: {w3.eth.block_number}")

    max_block = get_latest_block_from_postgres()
    start_block = (max_block + 1) if max_block else w3.eth.block_number
    block_ts_cache = {}

    print(f"▶️ Starting at block {start_block} (Latest in PostgreSQL: {max_block})")

    while True:
        try:
            head_block = w3.eth.block_number
            effective_end = max(0, head_block - CONFIRMATIONS)

            if start_block > effective_end:
                print(f"⏸ No new blocks yet. Head={head_block}, waiting {POLL_INTERVAL}s...")
                time.sleep(POLL_INTERVAL)
                continue

            total_blocks = effective_end - start_block + 1
            total_batches = (total_blocks + BATCH_SIZE - 1) // BATCH_SIZE

            for b_idx, batch_start in enumerate(
                    range(start_block, effective_end + 1, BATCH_SIZE), start=1
            ):
                batch_end = min(batch_start + BATCH_SIZE - 1, effective_end)
                print(f"\n--- Batch {batch_start} to {batch_end} ({b_idx}/{total_batches}) ---")

                docs_to_upsert = {}

                try:
                    logs = with_retries(
                        w3.eth.get_logs,
                        {
                            "fromBlock": batch_start,
                            "toBlock": batch_end,
                            "topics": [[TRANSFER_TOPIC, NATIVE_RON_TRANSFER_TOPIC]],
                        },
                        retries=3,
                        delay=5,
                    )
                except Exception as e:
                    print(f"❌ Failed to fetch logs for batch {batch_start}-{batch_end}: {e}")
                    continue

                for log in logs:
                    doc_id = f"{log['transactionHash'].hex()}-{log['logIndex']}"
                    log_topic = log["topics"][0].hex()

                    if log_topic == TRANSFER_TOPIC:
                        frm = Web3.to_checksum_address("0x" + log["topics"][1].hex()[-40:])
                        to = Web3.to_checksum_address("0x" + log["topics"][2].hex()[-40:])
                        if (frm in TREASURY_SET or to in TREASURY_SET) and not (
                                frm in TREASURY_SET and to in TREASURY_SET
                        ):
                            docs_to_upsert[doc_id] = {"type": "erc20_log", "data": log}

                    elif log_topic == NATIVE_RON_TRANSFER_TOPIC:
                        log_emitter = Web3.to_checksum_address(log["address"])
                        if log_emitter in TREASURY_SET:
                            docs_to_upsert[doc_id] = {"type": "native_ron_log", "data": log}

                print(f"Total unique events found in batch: {len(docs_to_upsert)}")
                if not docs_to_upsert:
                    start_block = batch_end + 1
                    continue

                values_to_upsert = []

                for item in docs_to_upsert.values():
                    log = item["data"]
                    try:
                        tx_hash = log["transactionHash"].hex()
                        tx_details = with_retries(w3.eth.get_transaction, tx_hash, retries=3, delay=5)

                        bn = log["blockNumber"]
                        if bn not in block_ts_cache:
                            block = with_retries(w3.eth.get_block, bn, retries=3, delay=5)
                            block_ts_cache[bn] = block["timestamp"]
                        block_timestamp = block_ts_cache[bn]

                        is_out = False
                        if item["type"] == "erc20_log":
                            frm = Web3.to_checksum_address("0x" + log["topics"][1].hex()[-40:])
                            to = Web3.to_checksum_address("0x" + log["topics"][2].hex()[-40:])
                            is_out = frm in TREASURY_SET
                            amt_raw = int(log["data"].hex(), 16)
                            token_addr = Web3.to_checksum_address(log["address"])

                        elif item["type"] == "native_ron_log":
                            to = Web3.to_checksum_address(log["address"])
                            is_out = False
                            amt_raw = int(log["data"].hex(), 16)
                            frm = Web3.to_checksum_address("0x" + log["topics"][1].hex()[-40:])
                            token_addr = "0x0000000000000000000000000000000000000000"

                        doc_id = f"{log['blockNumber']}-{log['logIndex']}"
                        amt = -amt_raw if is_out else amt_raw
                        method = safe_selector(tx_details.get("input", ""))
                        event_topic = log["topics"][0].hex()

                        values_to_upsert.append((
                            doc_id,
                            tx_hash,
                            int(bn),
                            int(log["logIndex"]),
                            int(block_timestamp),
                            str(frm),
                            str(to),
                            str(token_addr),
                            Decimal(str(amt_raw)),
                            Decimal(str(amt)),
                            bool(is_out),
                            str(method),
                            str(event_topic)
                        ))

                    except Exception as e:
                        print(f"❌ Error processing tx {log['transactionHash'].hex()}: {e}")

                if values_to_upsert:
                    try:
                        with get_cursor() as cur:
                            psycopg2.extras.execute_values(cur, """
                                INSERT INTO "txns" (
                                    "id", "tx_hash", "block_number", "log_index", "timestamp",
                                    "from_address", "to_address", "token_address", "amount_raw",
                                    "amount", "is_outgoing", "method", "event_topic"
                                )
                                VALUES %s
                                ON CONFLICT ("id") DO UPDATE SET
                                    "tx_hash" = EXCLUDED."tx_hash",
                                    "block_number" = EXCLUDED."block_number",
                                    "log_index" = EXCLUDED."log_index",
                                    "timestamp" = EXCLUDED."timestamp",
                                    "from_address" = EXCLUDED."from_address",
                                    "to_address" = EXCLUDED."to_address",
                                    "token_address" = EXCLUDED."token_address",
                                    "amount_raw" = EXCLUDED."amount_raw",
                                    "amount" = EXCLUDED."amount",
                                    "is_outgoing" = EXCLUDED."is_outgoing",
                                    "method" = EXCLUDED."method",
                                    "event_topic" = EXCLUDED."event_topic";
                            """, values_to_upsert, page_size=len(values_to_upsert))
                        start_block = batch_end + 1
                    except Exception as e:
                        print(f"❌ PostgreSQL write failed: {e}")
                        print("⚠️ Skipping block increment due to DB write errors.")
                else:
                    start_block = batch_end + 1

            print(f"✅ Finished cycle up to block {effective_end}. Sleeping {CYCLE_DELAY}s...")
            time.sleep(CYCLE_DELAY)

        except Exception as e:
            print(f"💥 Fatal error: {e}. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

if __name__ == "__main__":
    while True:
        try:
            process_and_store_transactions()
        except Exception as e:
            print(f"🔁 Restarting listener due to error: {e}")
            time.sleep(RETRY_DELAY)
