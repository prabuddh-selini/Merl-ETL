#!/usr/bin/env python3
"""
MerlinScan -> Postgres ETL
- Fills these tables you created:
  A) merlin_chain.block_check
  B) merlin_chain.holders_raw
  C) merlin_chain.wallet_transactions
  D) merlin_chain.ingestion_cursors (bookkeeping)
  E) merlin_chain.job_runs (audit)

Usage examples:
  # 1) Block-by-time single probe (timestamp -> block number)
  python merlin_etl.py block_by_time --unix-ts 1726224000 --closest after

  # 2) Token holders snapshot (6h bucket)
  python merlin_etl.py holders_snapshot \
      --token 0xYourTokenAddress \
      --bucket-start-utc "2025-09-16T12:00:00Z" \
      --trigger-id "2025/09/16-12:00" \
      --page-size 100

  # 3) Wallet token transfers (paged, with resume cursor)
  python merlin_etl.py wallet_tokentx \
      --wallet 0xYourWallet \
      --token 0xYourTokenAddress \
      --startblock 0 \
      --page-size 100

Environment variables required:
  DATABASE_URL     e.g. postgres://user:pass@localhost:5432/dbname
  MERLINSCAN_API_KEY
Optional:
  MERLINSCAN_BASE_URL (default: https://scan.merlinchain.io/api)
  ETL_RATE_LIMIT_QPS (default: 3.0)
"""

import os
import sys
import time
import json
import math
import argparse
import datetime as dt
from typing import Optional, Dict, Any, List

import psycopg2
import psycopg2.extras
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

BASE_URL = os.getenv("MERLINSCAN_BASE_URL", "https://scan.merlinchain.io/api")
API_KEY  = os.environ.get("MERLINSCAN_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")
QPS = float(os.getenv("ETL_RATE_LIMIT_QPS", "3.0"))  # ~3 requests/sec default
MIN_INTERVAL = 1.0 / max(QPS, 0.1)


# ----------------------------- HTTP Session with retries -----------------------------

def make_session() -> requests.Session:
    if not API_KEY:
        raise SystemExit("MERLINSCAN_API_KEY is not set in environment")
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=100)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

_last_call = 0.0
def ratelimit_sleep():
    global _last_call
    now = time.time()
    elapsed = now - _last_call
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_call = time.time()


def get_json(session: requests.Session, params: Dict[str, Any]) -> Dict[str, Any]:
    ratelimit_sleep()
    # Ensure api_key present
    p = dict(params)
    p["api_key"] = API_KEY
    r = session.get(BASE_URL, params=p, timeout=30)
    # If 429, brief extra sleep
    if r.status_code == 429:
        time.sleep(2.0)
    r.raise_for_status()
    try:
        data = r.json()
    except Exception as e:
        raise RuntimeError(f"Non-JSON response: {r.text[:400]}") from e
    return data


# ----------------------------- DB helpers -----------------------------

def pg_conn():
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL not set in environment")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False  # we'll manage transactions
    return conn


def execute(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)


def fetchone(conn, sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def fetchall(conn, sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


# ----------------------------- Job run audit -----------------------------

def job_start(conn, job_name: str, scheduled_for: Optional[str] = None) -> int:
    row = fetchone(conn, """
        INSERT INTO merlin_chain.job_runs (job_name, scheduled_for, started_at, status, stats)
        VALUES (%s, COALESCE(%s, now()), now(), 'success', '{}'::jsonb)
        RETURNING id
    """, (job_name, scheduled_for))
    conn.commit()
    return int(row["id"])


def job_finish(conn, job_id: int, status: str = "success", stats: Optional[dict] = None, error: Optional[str] = None):
    execute(conn, """
        UPDATE merlin_chain.job_runs
           SET finished_at = now(),
               status = %s,
               stats = COALESCE(%s::jsonb, stats),
               error = COALESCE(%s, error)
         WHERE id = %s
    """, (status, json.dumps(stats or {}), error, job_id))
    conn.commit()


# ----------------------------- Cursors -----------------------------

def cursor_get(conn, stream: str) -> Optional[dict]:
    return fetchone(conn, "SELECT * FROM merlin_chain.ingestion_cursors WHERE stream=%s", (stream,))
def cursor_upsert(conn, stream: str, last_block: Optional[int], last_page: Optional[int], metadata: Optional[dict] = None):
    execute(conn, """
        INSERT INTO merlin_chain.ingestion_cursors (stream, last_scanned_block, last_page, metadata, updated_at_utc)
        VALUES (%s, %s, %s, COALESCE(%s::jsonb,'{}'::jsonb), now())
        ON CONFLICT (stream) DO UPDATE SET
            last_scanned_block = GREATEST(
                COALESCE(merlin_chain.ingestion_cursors.last_scanned_block, 0),
                COALESCE(EXCLUDED.last_scanned_block, 0)
            ),
            last_page = GREATEST(
                COALESCE(merlin_chain.ingestion_cursors.last_page, 0),
                COALESCE(EXCLUDED.last_page, 0)
            ),
            metadata       = EXCLUDED.metadata,
            updated_at_utc = now()
    """, (stream, last_block, last_page, json.dumps(metadata or {})))
    conn.commit()


# ----------------------------- Utilities -----------------------------

def ts_to_utc(ts_int: int) -> str:
    # Return ISO UTC string compatible with timestamptz
    return dt.datetime.utcfromtimestamp(int(ts_int)).replace(tzinfo=dt.timezone.utc).isoformat()

def parse_int(s: Any, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default

def floor_to_6h(utc: dt.datetime) -> dt.datetime:
    # Floor to 00:00, 06:00, 12:00, 18:00 UTC
    h = utc.hour
    bucket_hour = (h // 6) * 6
    return utc.replace(hour=bucket_hour, minute=0, second=0, microsecond=0, tzinfo=dt.timezone.utc)


# ----------------------------- A) block_by_time -----------------------------

def etl_block_by_time(unix_ts: int, closest: str = "after") -> dict:
    session = make_session()
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": str(unix_ts),
        "closest": closest
    }
    data = get_json(session, params)
    status = data.get("status")
    message = data.get("message")
    result = data.get("result")

    block_number = parse_int(result, None)
    ts_utc = ts_to_utc(unix_ts)

    conn = pg_conn()
    job_id = job_start(conn, "block_check_hourly")

    try:
        execute(conn, """
            INSERT INTO merlin_chain.block_check
            (unix_ts, ts_utc, closest, block_number, api_status, api_message, inserted_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (unix_ts, closest) DO UPDATE SET
                block_number = EXCLUDED.block_number,
                api_status = EXCLUDED.api_status,
                api_message = EXCLUDED.api_message
        """, (unix_ts, ts_utc, closest, block_number, status, message))
        conn.commit()
        job_finish(conn, job_id, "success", {"block_number": block_number, "status": status, "message": message})
    except Exception as e:
        conn.rollback()
        job_finish(conn, job_id, "error", {"status": status, "message": message}, error=str(e))
        raise
    finally:
        conn.close()
    return {"block_number": block_number, "status": status, "message": message}


# ----------------------------- B) holders_snapshot -----------------------------

def etl_holders_snapshot(token: str, bucket_start_utc_str: str, trigger_id: str, page_size: int = 100, max_pages: int = 10000, default_decimals: int = 18) -> dict:
    """
    Pull pages from tokenholderlist and upsert into holders_raw.
    """
    session = make_session()
    conn = pg_conn()
    job_id = job_start(conn, "holders_6h", scheduled_for=bucket_start_utc_str)

    total_rows = 0
    pages = 0
    decimals = default_decimals

    try:
        for page in range(1, max_pages + 1):
            params = {
                "module": "token",
                "action": "tokenholderlist",
                "contractaddress": token,
                "page": page,
                "offset": page_size,
            }
            data = get_json(session, params)
            status = data.get("status")
            message = data.get("message")
            result = data.get("result")

            # End conditions
            if status != "1" or not result:
                break

            # Some explorers include decimals on each item, some do not.
            # We'll try to detect once.
            if isinstance(result, list):
                batch = []
                for it in result:
                    holder_addr = (
                        it.get("TokenHolderAddress") or it.get("tokenHolderAddress")
                        or it.get("HolderAddress")   or it.get("holderAddress")
                        or it.get("address")
                    )
                    qty_raw_str = it.get("TokenHolderQuantity") or it.get("quantity") or it.get("balance")

                    # decimals might appear as 'decimals' or 'tokenDecimal'
                    item_dec = it.get("decimals") or it.get("tokenDecimal")
                    if item_dec is not None:
                        try:
                            decimals = int(item_dec)
                        except Exception:
                            pass

                    if not holder_addr or qty_raw_str is None:
                        continue

                    batch.append((
                        trigger_id,
                        bucket_start_utc_str,
                        token.lower(),
                        holder_addr.lower(),
                        decimals,
                        str(qty_raw_str),
                    ))

                if batch:
                    psycopg2.extras.execute_values(
                        conn.cursor(),
                        """
                        INSERT INTO merlin_chain.holders_raw
                        (trigger_id, bucket_start_utc, contract_address, holder_address, token_decimal, quantity_raw)
                        VALUES %s
                        ON CONFLICT (bucket_start_utc, contract_address, holder_address) DO UPDATE SET
                            token_decimal = EXCLUDED.token_decimal,
                            quantity_raw = EXCLUDED.quantity_raw,
                            updated_at = now()
                        """,
                        batch,
                        page_size=100
                    )
                    conn.commit()
                    total_rows += len(batch)

                    # ---- ADDED PROGRESS LOG (holders) ----
                    print(f"[holders_snapshot] page={page} rows_added={len(batch)} total_rows={total_rows}", flush=True)
                    # --------------------------------------

                pages += 1

                # If fewer than page_size, we've reached the end
                if len(result) < page_size:
                    break
            else:
                break

        job_finish(conn, job_id, "success", {"rows": total_rows, "pages": pages, "token": token, "decimals": decimals})
    except Exception as e:
        conn.rollback()
        job_finish(conn, job_id, "error", {"rows": total_rows, "pages": pages, "token": token}, error=str(e))
        raise
    finally:
        conn.close()

    return {"rows": total_rows, "pages": pages, "token": token, "decimals": decimals}


# ----------------------------- C) wallet_tokentx -----------------------------

def etl_wallet_tokentx(wallet: str, token: str, startblock: int = 0, page_size: int = 100, max_pages: int = 100000) -> dict:
    """
    Paginates tokentx for (wallet, token); resumes via ingestion_cursors stream key.
    Stream key format: f"tokentx:{wallet}:{token}"
    """
    session = make_session()
    conn = pg_conn()
    stream = f"tokentx:{wallet.lower()}:{token.lower()}"
    cur = cursor_get(conn, stream)
    last_page = cur["last_page"] if cur and cur.get("last_page") is not None else None
    # We respect provided startblock, but if cursor saved a last_scanned_block, prefer max.
    last_block = cur["last_scanned_block"] if cur and cur.get("last_scanned_block") is not None else startblock

    job_id = job_start(conn, "wallet_tx_hourly")

    total_rows = 0
    pages = 0
    current_page = 1

    try:
        while pages < max_pages:
            params = {
                "module": "account",
                "action": "tokentx",
                "address": wallet,
                "contractaddress": token,
                "startblock": last_block,
                "endblock": 99999999,
                "sort": "asc",
                "page": current_page,
                "offset": page_size,
            }
            data = get_json(session, params)
            status = data.get("status")
            message = data.get("message")
            result = data.get("result")

            if status != "1" or not isinstance(result, list) or len(result) == 0:
                # Commit cursor anyway
                cursor_upsert(conn, stream, last_block, 0, {"status": status, "message": message})
                break

            # Insert batch
            batch = []
            for it in result:
                blockNumber = parse_int(it.get("blockNumber"))
                timeStamp = parse_int(it.get("timeStamp"))
                tx_hash = it.get("hash")
                from_addr = (it.get("from") or "").lower()
                to_addr = (it.get("to") or "").lower()
                value_raw = it.get("value") or "0"
                tokenName = it.get("tokenName")
                tokenSymbol = it.get("tokenSymbol")
                tokenDecimal = parse_int(it.get("tokenDecimal"), 18)
                nonce = parse_int(it.get("nonce"), None)
                blockHash = it.get("blockHash")
                txIndex = parse_int(it.get("transactionIndex"), 0)
                gas = parse_int(it.get("gas"), None)
                gasPrice = it.get("gasPrice")
                gasUsed = parse_int(it.get("gasUsed"), None)
                cumulativeGasUsed = parse_int(it.get("cumulativeGasUsed"), None)
                input_data = it.get("input")
                confirmations = parse_int(it.get("confirmations"), None)

                batch.append((
                    wallet.lower(),
                    None,  # wallet_name (enrichment optional)
                    None,  # wallet_tag
                    token.lower(),
                    blockNumber,
                    timeStamp,
                    ts_to_utc(timeStamp),
                    tx_hash,
                    nonce,
                    blockHash,
                    from_addr,
                    to_addr,
                    str(value_raw),
                    tokenName,
                    tokenSymbol,
                    tokenDecimal,
                    txIndex,
                    gas,
                    gasPrice,
                    gasUsed,
                    cumulativeGasUsed,
                    input_data,
                    confirmations,
                ))
                last_block = max(last_block, blockNumber)

            if batch:
                psycopg2.extras.execute_values(
                    conn.cursor(),
                    """
                    INSERT INTO merlin_chain.wallet_transactions
                    (wallet_address, wallet_name, wallet_tag, contract_address, block_number, block_time_unix, block_time_utc,
                    tx_hash, nonce, block_hash, from_address, to_address, value_raw, token_name, token_symbol, token_decimal,
                    transaction_index, gas, gas_price, gas_used, cumulative_gas_used, input, confirmations)
                    VALUES %s
                    ON CONFLICT (tx_hash, contract_address, wallet_address, value_raw, COALESCE(transaction_index, 0)) DO NOTHING
                    """,
                    batch,
                    page_size=100
                )
                conn.commit()
                total_rows += len(batch)

                # ---- ADDED PROGRESS LOG (tokentx) ----
                print(
                    f"[wallet_tokentx] page={current_page} rows_added={len(batch)} "
                    f"total_rows={total_rows} last_block={last_block}",
                    flush=True
                )
                # --------------------------------------

            # Save cursor every page
            cursor_upsert(conn, stream, last_block, current_page, {"status": status, "message": message})

            pages += 1
            # Stop if fewer than page_size → end reached
            if len(result) < page_size:
                break
            current_page += 1

        job_finish(conn, job_id, "success", {"rows": total_rows, "pages": pages, "wallet": wallet, "token": token, "last_block": last_block})
    except Exception as e:
        conn.rollback()
        job_finish(conn, job_id, "error", {"rows": total_rows, "pages": pages, "wallet": wallet, "token": token, "last_block": last_block}, error=str(e))
        raise
    finally:
        conn.close()

    return {"rows": total_rows, "pages": pages, "wallet": wallet, "token": token, "last_block": last_block}


# ----------------------------- CLI -----------------------------

def main():
    parser = argparse.ArgumentParser(description="MerlinScan -> Postgres ETL")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # block_by_time
    p1 = sub.add_parser("block_by_time", help="Insert block number for a unix timestamp into block_check")
    p1.add_argument("--unix-ts", type=int, required=True)
    p1.add_argument("--closest", choices=["before", "after"], default="after")

    # holders_snapshot
    p2 = sub.add_parser("holders_snapshot", help="Snapshot token holders into holders_raw")
    p2.add_argument("--token", required=True, help="Token contract address")
    p2.add_argument("--bucket-start-utc", required=True, help="UTC ISO time e.g. 2025-09-16T12:00:00Z")
    p2.add_argument("--trigger-id", required=True, help="Human-friendly bucket id e.g. 2025/09/16-12:00")
    p2.add_argument("--page-size", type=int, default=100)
    p2.add_argument("--max-pages", type=int, default=10000)
    p2.add_argument("--default-decimals", type=int, default=18)

    # wallet_tokentx
    p3 = sub.add_parser("wallet_tokentx", help="Ingest tokentx for a wallet+token into wallet_transactions (paged, resumable)")
    p3.add_argument("--wallet", required=True)
    p3.add_argument("--token", required=True)
    p3.add_argument("--startblock", type=int, default=0)
    p3.add_argument("--page-size", type=int, default=100)
    p3.add_argument("--max-pages", type=int, default=100000)

    args = parser.parse_args()

    if args.cmd == "block_by_time":
        out = etl_block_by_time(args.unix_ts, args.closest)
    elif args.cmd == "holders_snapshot":
        out = etl_holders_snapshot(args.token, args.bucket_start_utc, args.trigger_id, args.page_size, args.max_pages, args.default_decimals)
    elif args.cmd == "wallet_tokentx":
        out = etl_wallet_tokentx(args.wallet, args.token, args.startblock, args.page_size, args.max_pages)
    else:
        raise SystemExit("Unknown command")

    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
