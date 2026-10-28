import os
import sys
import json
from decimal import Decimal, getcontext
from dotenv import load_dotenv
import psycopg2.extras

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from db import get_cursor

load_dotenv()

RON_ZERO = "0x0000000000000000000000000000000000000000"
RON_ALIASES = {RON_ZERO, "0xe514d9deb7966c8be0ca922de8a064264ea6bcd4"}

FALLBACK_DECIMALS = {"RON": 18, "WETH": 18, "AXS": 18, "USDC": 6}

getcontext().prec = 50

def _lower(s):
    return s.lower() if isinstance(s, str) else s

def canon_token_address(addr: str) -> str:
    a = _lower(addr)
    return RON_ZERO if a in {_lower(x) for x in RON_ALIASES} else a

def load_token_symbol_map(cur):
    cur.execute('SELECT "data" FROM "tokens" WHERE "id" = %s LIMIT 1;', ("token_symbol_map",))
    row = cur.fetchone()
    if not row or not row[0]:
        raise RuntimeError("token_symbol_map not found in 'tokens' table.")

    tokens_dict = row[0]
    m = {_lower(a): sym for a, sym in tokens_dict.items()}

    ron_sym = m.get(_lower(RON_ZERO), "RON")
    for alias in RON_ALIASES:
        m[_lower(alias)] = ron_sym

    whitelist = set(m.keys())
    return m, whitelist

def load_selector_to_category(cur):
    sel2cat = {}
    cur.execute('SELECT "id", "data" FROM "methods";')
    rows = cur.fetchall()

    for doc_id, doc in rows:
        if not isinstance(doc, dict):
            continue
        had_arrays = False
        for k, v in doc.items():
            if isinstance(v, list) and v and all(isinstance(x, str) for x in v):
                cat = str(k)
                for sel in v:
                    s = _lower(sel)
                    if isinstance(s, str) and s.startswith("0x"):
                        sel2cat[s] = cat
                had_arrays = True
        if had_arrays:
            continue

        selector = doc.get("selector") or doc.get("typeName") or doc_id
        category = doc.get("category") or doc.get("group") or doc.get("bucket") or doc.get("name")
        if isinstance(selector, str) and isinstance(category, str) and category.strip():
            sel2cat[_lower(selector)] = category.strip()

    return sel2cat

def decimals_for_address(addr_lower: str, addr2sym: dict) -> int:
    sym = addr2sym.get(addr_lower)
    return FALLBACK_DECIMALS.get(sym, 18) if sym else 18

def main():
    print("Iniciando agregação typeData (PostgreSQL)...")
    with get_cursor() as cur:
        addr2sym, whitelist = load_token_symbol_map(cur)
        selector2cat = load_selector_to_category(cur)

        canonical_whitelist = set()
        for a in whitelist:
            canonical_whitelist.add(_lower(canon_token_address(a)))

        agg = {}

        cur.execute('SELECT "date", "entries" FROM "dailyData";')
        daily_rows = cur.fetchall()

        for date_str, entries in daily_rows:
            if not isinstance(entries, list):
                continue

            for e in entries:
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

        # Atualiza a tabela typeData
        cur.execute("""
            INSERT INTO "typeData" ("id", "Breeding", "Core", "Marketplace", "Origins", "Other", "Pack", "bAXS")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("id") DO UPDATE SET
                "Breeding" = EXCLUDED."Breeding",
                "Core" = EXCLUDED."Core",
                "Marketplace" = EXCLUDED."Marketplace",
                "Origins" = EXCLUDED."Origins",
                "Other" = EXCLUDED."Other",
                "Pack" = EXCLUDED."Pack",
                "bAXS" = EXCLUDED."bAXS";
        """, (
            "token_type_aggregation",
            psycopg2.extras.Json(out_doc.get("Breeding")) if out_doc.get("Breeding") else None,
            psycopg2.extras.Json(out_doc.get("Core")) if out_doc.get("Core") else None,
            psycopg2.extras.Json(out_doc.get("Marketplace")) if out_doc.get("Marketplace") else None,
            psycopg2.extras.Json(out_doc.get("Origins")) if out_doc.get("Origins") else None,
            psycopg2.extras.Json(out_doc.get("Other")) if out_doc.get("Other") else None,
            psycopg2.extras.Json(out_doc.get("Pack")) if out_doc.get("Pack") else None,
            psycopg2.extras.Json(out_doc.get("bAXS")) if out_doc.get("bAXS") else None,
        ))

        print(f"✅ Documento atualizado em 'typeData' com {len(out_doc)} categorias.")
        print(f"Categorias agregadas: {list(out_doc.keys())}")

if __name__ == "__main__":
    main()
