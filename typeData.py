import os
from decimal import Decimal, getcontext
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in your environment.")

MONGO_DB = "data"
MONGO_COLLECTION = "dailyData"
TARGET_COLLECTION = "typeData"
TOKENS_COLLECTION = "tokens"
METHODS_COLLECTION = "methods"

RON_ZERO = "0x0000000000000000000000000000000000000000"
RON_ALIASES = {RON_ZERO, "0xe514d9deb7966c8be0ca922de8a064264ea6bcd4"}

FALLBACK_DECIMALS = {"RON": 18, "WETH": 18, "AXS": 18, "USDC": 6}

getcontext().prec = 50

def _lower(s):
    return s.lower() if isinstance(s, str) else s

def canon_token_address(addr: str) -> str:
    a = _lower(addr)
    return RON_ZERO if a in {_lower(x) for x in RON_ALIASES} else a

def load_token_symbol_map(tokens_col):
    doc = tokens_col.find_one({"_id": "token_symbol_map"})
    if not doc or "tokens" not in doc or not isinstance(doc["tokens"], dict):
        raise RuntimeError("token_symbol_map not found or malformed in 'tokens'.")

    m = {_lower(a): sym for a, sym in doc["tokens"].items()}

    ron_sym = m.get(_lower(RON_ZERO), "RON")
    for alias in RON_ALIASES:
        m[_lower(alias)] = ron_sym

    whitelist = set(m.keys())
    return m, whitelist

def load_selector_to_category(methods_col):
    sel2cat = {}
    cursor = methods_col.find({})
    for doc in cursor:
        had_arrays = False
        for k, v in doc.items():
            if k == "_id":
                continue
            if isinstance(v, list) and v and all(isinstance(x, str) for x in v):
                cat = str(k)
                for sel in v:
                    s = _lower(sel)
                    if isinstance(s, str) and s.startswith("0x"):
                        sel2cat[s] = cat
                had_arrays = True
        if had_arrays:
            continue

        selector = doc.get("selector") or doc.get("typeName") or doc.get("_id")
        category = doc.get("category") or doc.get("group") or doc.get("bucket") or doc.get("name")
        if isinstance(selector, str) and isinstance(category, str) and category.strip():
            sel2cat[_lower(selector)] = category.strip()

    return sel2cat

def decimals_for_address(addr_lower: str, addr2sym: dict) -> int:
    sym = addr2sym.get(addr_lower)
    return FALLBACK_DECIMALS.get(sym, 18) if sym else 18

def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    daily_col = db[MONGO_COLLECTION]
    target_col = db[TARGET_COLLECTION]
    tokens_col = db[TOKENS_COLLECTION]
    methods_col = db[METHODS_COLLECTION]

    addr2sym, whitelist = load_token_symbol_map(tokens_col)
    selector2cat = load_selector_to_category(methods_col)

    canonical_whitelist = set()
    for a in whitelist:
        canonical_whitelist.add(_lower(canon_token_address(a)))

    agg = {}

    for doc in daily_col.find({}):
        for k, v in doc.items():
            if k == "_id" or not isinstance(v, list):
                continue

            for e in v:
                if not isinstance(e, dict):
                    continue

                selector = e.get("typeName")
                category = selector2cat.get(_lower(selector), "Other") if isinstance(selector, str) else "Other"

                token_raw = e.get("token")
                if not isinstance(token_raw, str):
                    continue

                token_addr = canon_token_address(token_raw)
                token_addr_lower = _lower(token_addr)

                if token_addr_lower not in canonical_whitelist:
                    continue

                sum_obj = e.get("_sum", {})
                raw_val = sum_obj.get("value")
                if raw_val is None:
                    continue

                try:
                    raw_dec = Decimal(str(raw_val))
                except Exception:
                    continue

                decs = decimals_for_address(token_addr_lower, addr2sym)
                amount = raw_dec / (Decimal(10) ** decs)

                bucket = agg.setdefault(category, {})
                bucket[token_addr_lower] = bucket.get(token_addr_lower, Decimal(0)) + amount

    out_doc = {
        cat: {addr: float(val) for addr, val in token_map.items()}
        for cat, token_map in agg.items()
    }

    result = target_col.update_one(
        {"_id": "token_type_aggregation"},
        {"$set": out_doc},
        upsert=True,
    )

    if result.matched_count == 0 and result.upserted_id:
        print(f"✅ Inserted new aggregation document with _id={result.upserted_id}")
    elif result.modified_count > 0:
        print(f"✅ Updated existing aggregation document in '{TARGET_COLLECTION}' with {len(out_doc)} categories.")
    else:
        print("ℹ️ Aggregation ran, but no changes were made (data identical).")

    if out_doc:
        print(f"Categories aggregated: {list(out_doc.keys())}")
    else:
        print("⚠️ No categories were aggregated (empty result).")

if __name__ == "__main__":
    main()
