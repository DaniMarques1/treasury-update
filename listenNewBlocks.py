import os
import time
from web3 import Web3
from web3.middleware import geth_poa_middleware
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, OperationFailure
from dotenv import load_dotenv

load_dotenv()

# RONIN_RPC = "http://127.0.0.1:8545"
RONIN_RPC = "https://api.roninchain.com/rpc"

BATCH_SIZE = 100
CONFIRMATIONS = 1
POLL_INTERVAL = 20
RETRY_DELAY = 10
CYCLE_DELAY = 60  # wait after finishing a full cycle

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = "data"
MONGO_COLLECTION = "txns"

TREASURY_ADDRESSES = [
    Web3.to_checksum_address("0x245db945c485b68fDc429e4F7085a1761Aa4d45d"),
]
TREASURY_SET = set(TREASURY_ADDRESSES)

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
NATIVE_RON_TRANSFER_TOPIC = "0x3d0ce9bfc3ed7d6862dbb28b2dea94561fe714a1b4d019aa8af39730d1ad7c3d"


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
    """Run fn with retries for transient errors (RPC issues, network hiccups)."""
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            print(f"⚠️ Attempt {attempt}/{retries} failed for {fn.__name__}: {e}")
            if attempt == retries:
                raise
            time.sleep(delay)


def process_and_store_transactions():
    w3 = Web3(Web3.HTTPProvider(RONIN_RPC))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    if not w3.is_connected():
        raise Exception(f"❌ Failed to connect to Ronin node at {RONIN_RPC}")
    print(f"✅ Connected to Ronin. Head block: {w3.eth.block_number}")

    if not MONGO_URI:
        raise Exception("❌ MONGO_URI not set.")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        print("✅ Connected to MongoDB.")
    except (ConnectionFailure, OperationFailure) as e:
        raise Exception(f"❌ MongoDB connection failed: {e}")

    last_doc = collection.find_one(
        sort=[("blockNumber", -1)], projection={"blockNumber": 1, "_id": 0}
    )
    start_block = (
        last_doc["blockNumber"] + 1 if last_doc else w3.eth.block_number
    )
    block_ts_cache = {}

    print(f"▶️ Starting at block {start_block}")

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
                    continue

                ops = []

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

                        doc = None
                        is_out = False

                        if item["type"] == "erc20_log":
                            frm = Web3.to_checksum_address("0x" + log["topics"][1].hex()[-40:])
                            to = Web3.to_checksum_address("0x" + log["topics"][2].hex()[-40:])
                            is_out = frm in TREASURY_SET
                            amt_raw = int(log["data"].hex(), 16)
                            doc = {
                                "tokenAddress": Web3.to_checksum_address(log["address"]),
                                "fromAddress": frm,
                                "toAddress": to,
                            }

                        elif item["type"] == "native_ron_log":
                            to_addr = Web3.to_checksum_address(log["address"])
                            is_out = False
                            amt_raw = int(log["data"].hex(), 16)
                            from_addr = Web3.to_checksum_address("0x" + log["topics"][1].hex()[-40:])
                            doc = {
                                "tokenAddress": "0x0000000000000000000000000000000000000000",
                                "fromAddress": from_addr,
                                "toAddress": to_addr,
                            }

                        if doc:
                            doc.update(
                                {
                                    "_id": f"{log['blockNumber']}-{log['logIndex']}",
                                    "txHash": tx_hash,
                                    "blockNumber": bn,
                                    "timestamp": block_timestamp,
                                    "logIndex": log["logIndex"],
                                    "eventTopic": log["topics"][0].hex(),
                                    "amountRaw": str(amt_raw),
                                    "amount": str(-amt_raw if is_out else amt_raw),
                                    "isOutgoing": is_out,
                                    "method": safe_selector(tx_details.get("input", "")),
                                }
                            )
                            ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))

                    except Exception as e:
                        print(f"❌ Error processing tx {log['transactionHash'].hex()}: {e}")
                        # requeue this log for the next batch retry
                        docs_to_upsert[log["transactionHash"].hex()] = item

                if ops:
                    try:
                        collection.bulk_write(ops, ordered=False)
                        start_block = batch_end + 1  # advance only on success
                    except Exception as e:
                        print(f"❌ Bulk write failed: {e}")
                        print("⚠️ Skipping block increment due to Mongo write errors.")

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
