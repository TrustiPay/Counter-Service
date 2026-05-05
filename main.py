from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ValidationError
import httpx
import hashlib
import hmac
import json
import asyncio
import time
import os
from secrets import compare_digest
from typing import Optional
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, firestore

load_dotenv()

COUNTER_URL = os.getenv("COUNTER_URL", "https://counter.trustipay.online")
CENTRAL_LEDGER_URL = os.getenv("CENTRAL_LEDGER_URL", "http://central-ledger-service:8001")
FRAUD_MODEL_URL = os.getenv("FRAUD_MODEL_URL", "http://fraud-model-service:8002")
FIREBASE_CREDENTIALS_JSON = os.getenv("FIREBASE_CREDENTIALS_JSON")
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "5"))

HMAC_SECRET = "trustipay_demo_secret"

KEY_MAP = {
    "t": "tx_id", "s": "sender_id", "r": "receiver_id",
    "ts": "timestamp", "a": "amount", "c": "category",
    "l": "location", "d": "device_id", "dt": "device_type",
    "nt": "network_type", "n": "nonce", "p": "prev_hash", "sig": "signature",
}

# Queues
tx_queue: asyncio.Queue = asyncio.Queue()      # offline Firebase transactions
fraud_queue: asyncio.Queue = asyncio.Queue()   # transactions awaiting fraud check

_db = None
_http: httpx.AsyncClient = None
_current_fraud_task: Optional[dict] = None        # tracks the single in-flight fraud check


def init_firebase():
    global _db
    try:
        if not FIREBASE_CREDENTIALS_JSON:
            raise ValueError("FIREBASE_CREDENTIALS_JSON env var is not set")
        cred = credentials.Certificate(json.loads(FIREBASE_CREDENTIALS_JSON))
        firebase_admin.initialize_app(cred)
        _db = firestore.client()
        print("[Firebase] Initialized")
    except Exception as e:
        print(f"[Firebase] Init failed: {e}")


# ── Blocking helpers (run in thread-pool executor) ────────────────────────────

def fetch_pending_transactions() -> list[dict]:
    if _db is None:
        return []
    docs = list(
        _db.collection("offline_transactions")
        .where("status", "==", "PENDING_SYNC")
        .stream()
    )
    result = []
    for doc in docs:
        tx = doc.to_dict()
        result.append(tx)
        doc.reference.update({"status": "Processing"})
    if result:
        print(f"[Firebase Sync] Fetched {len(result)} transaction(s)")
    return result


async def process_offline_transaction(tx: dict):
    tx_id = tx.get("tx_id")
    try:
        amount = float(tx.get("amount", 0))
        if not tx_id or amount <= 0:
            print(f"[Queue] Skipping invalid transaction {tx_id}")
            return

        payload = {
            "tx_id": tx_id,
            "sender_id": tx.get("sender_id"),
            "receiver_id": tx.get("receiver_id"),
            "timestamp": tx.get("timestamp"),
            "amount": amount,
            "category": tx.get("category"),
            "location": tx.get("location"),
            "device_id": tx.get("device_id"),
            "device_type": tx.get("device_type"),
            "network_type": tx.get("network_type"),
            "nonce": str(tx.get("nonce", "")),
            "prev_hash": tx.get("prev_hash"),
            "signature": tx.get("signature"),
        }

        if not verify_signature(payload):
            payload.update({"status": "REJECTED", "reason": "Invalid signature"})
            await append_transaction(payload)
            print(f"[Queue] {tx_id} rejected: invalid signature")
            return

        if await check_duplicate_tx(tx_id):
            payload.update({"status": "REJECTED", "reason": "Duplicate transaction"})
            await append_transaction(payload)
            print(f"[Queue] {tx_id} rejected: duplicate")
            return

        payload["status"] = "FRAUD_PENDING"
        await append_transaction(payload)
        await fraud_queue.put(payload)
        print(f"[Queue] {tx_id} queued for fraud check")

    except Exception as e:
        print(f"[Queue] Error on {tx_id}: {e}")


# ── Fraud check worker (processes one at a time) ──────────────────────────────

async def process_fraud_check(tx: dict):
    tx_id = tx.get("tx_id")
    try:
        sender, receiver = await asyncio.gather(
            get_user(tx["sender_id"]),
            get_user(tx["receiver_id"]),
        )

        fraud_payload = {
            **tx,
            "sender_current_balance": sender.get("current_balance"),
            "receiver_current_balance": receiver.get("current_balance"),
            "phone_number": sender.get("phone_number"),
        }

        fraud_response = await _http.post(f"{FRAUD_MODEL_URL}/fraud/check", json=fraud_payload)
        if fraud_response.status_code != 200:
            await update_transaction(tx_id, {"status": "REJECTED", "reason": "Fraud model error"})
            print(f"[Fraud Worker] {tx_id} → REJECTED (model error)")
            return

        fraud_result = fraud_response.json()
        fraud_status = fraud_result.get("status", "REJECTED")
        reason = fraud_result.get("reason", "")

        if fraud_status == "ACCEPTED":
            status = "APPROVED"
        elif fraud_status == "OTP_PENDING":
            status = "OTP_PENDING"
        else:
            status = "REJECTED"

        await update_transaction(tx_id, {"status": status, "reason": reason})
        print(f"[Fraud Worker] {tx_id} → {status}")

    except Exception as e:
        print(f"[Fraud Worker] Error on {tx_id}: {e}")
        try:
            await update_transaction(tx_id, {"status": "REJECTED", "reason": "Internal error"})
        except Exception:
            pass


# ── Async background loops ────────────────────────────────────────────────────

async def firebase_sync_loop():
    loop = asyncio.get_event_loop()
    print(f"[Firebase Sync] Started — interval: {SYNC_INTERVAL}s")
    while True:
        await asyncio.sleep(SYNC_INTERVAL)
        try:
            txs = await loop.run_in_executor(None, fetch_pending_transactions)
            for tx in txs:
                await tx_queue.put(tx)
        except Exception as e:
            print(f"[Firebase Sync] Error: {e}")


async def queue_worker_loop():
    print("[Queue Worker] Started")
    while True:
        tx = await tx_queue.get()
        try:
            await process_offline_transaction(tx)
        finally:
            tx_queue.task_done()


async def fraud_worker_loop():
    """Processes fraud checks one at a time to avoid overwhelming the model service."""
    global _current_fraud_task
    print("[Fraud Worker] Started")
    while True:
        tx = await fraud_queue.get()
        _current_fraud_task = {
            "tx_id": tx.get("tx_id"),
            "sender_id": tx.get("sender_id"),
            "amount": tx.get("amount"),
            "started_at": time.time(),
        }
        try:
            await process_fraud_check(tx)
        finally:
            _current_fraud_task = None
            fraud_queue.task_done()


# ── App lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http
    _http = httpx.AsyncClient(timeout=30.0)
    init_firebase()
    sync_task = asyncio.create_task(firebase_sync_loop())
    worker_task = asyncio.create_task(queue_worker_loop())
    fraud_task = asyncio.create_task(fraud_worker_loop())
    yield
    sync_task.cancel()
    worker_task.cancel()
    fraud_task.cancel()
    await asyncio.gather(sync_task, worker_task, fraud_task, return_exceptions=True)
    await _http.aclose()


app = FastAPI(lifespan=lifespan)


# ── Domain helpers ────────────────────────────────────────────────────────────

class MinifiedIOU(BaseModel):
    t: str
    s: int
    r: int
    ts: str
    a: float
    c: str
    l: str
    d: str
    dt: str
    nt: str
    n: str
    p: str
    sig: str


def expand_iou(data: MinifiedIOU) -> dict:
    return {KEY_MAP[k]: v for k, v in data.model_dump().items()}


def verify_signature(payload: dict) -> bool:
    signature = payload.get("signature")
    if not signature:
        return False
    body = json.dumps(
        {k: v for k, v in payload.items() if k != "signature"},
        separators=(",", ":"),
        sort_keys=True,
    )
    expected = hmac.new(HMAC_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    return compare_digest(expected, signature)


async def check_duplicate_tx(tx_id: str) -> bool:
    r = await _http.get(f"{CENTRAL_LEDGER_URL}/ledger/transactions/{tx_id}/exists")
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail="Central ledger service error")
    return r.json().get("exists", False)


async def check_sufficient_balance(sender_id, amount: float) -> bool:
    r = await _http.get(f"{CENTRAL_LEDGER_URL}/ledger/users/{sender_id}")
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail="Central ledger service error")
    return r.json().get("current_balance", 0) >= amount


async def get_user(user_id) -> dict:
    r = await _http.get(f"{CENTRAL_LEDGER_URL}/ledger/users/{user_id}")
    if r.status_code != 200:
        raise Exception("Error retrieving user details")
    return r.json()


async def append_transaction(transaction: dict) -> dict:
    r = await _http.post(f"{CENTRAL_LEDGER_URL}/ledger/transactions", json=transaction)
    if r.status_code != 200:
        raise Exception("Error appending transaction to ledger")
    return r.json()


async def update_transaction(tx_id: str, updates: dict):
    r = await _http.patch(f"{CENTRAL_LEDGER_URL}/ledger/transactions/{tx_id}", json=updates)
    if r.status_code not in (200, 204):
        raise Exception(f"Error updating transaction {tx_id}: {r.status_code}")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health_check():
    return {"status": "running", "queue_size": tx_queue.qsize()}


@app.get("/counter/queue/status")
def queue_status():
    return {"queue_size": tx_queue.qsize()}


@app.get("/counter/status")
def get_status():
    current = None
    if _current_fraud_task:
        current = {
            **_current_fraud_task,
            "elapsed_seconds": round(time.time() - _current_fraud_task["started_at"], 1),
        }
    return {
        "tx_queue_size": tx_queue.qsize(),
        "fraud_queue_size": fraud_queue.qsize(),
        "current_fraud_task": current,
    }


@app.get("/ledger-data")
async def get_ledger_data():
    r = await _http.get(f"{CENTRAL_LEDGER_URL}/ledger")
    return r.json()


@app.post("/counter/transactions")
async def process_transaction(iou: MinifiedIOU):
    try:
        expanded = expand_iou(iou)

        if expanded["amount"] <= 0:
            raise HTTPException(status_code=400, detail="Invalid transaction payload")

        if not verify_signature(expanded):
            expanded.update({"status": "REJECTED", "reason": "Invalid signature"})
            await append_transaction(expanded)
            return {"message": "Transaction rejected", "reason": "Invalid signature"}

        if await check_duplicate_tx(expanded["tx_id"]):
            expanded.update({"status": "REJECTED", "reason": "Duplicate transaction"})
            await append_transaction(expanded)
            return {"message": "Transaction rejected", "reason": "Duplicate transaction"}

        if not await check_sufficient_balance(expanded["sender_id"], expanded["amount"]):
            expanded.update({"status": "REJECTED", "reason": "Insufficient balance"})
            await append_transaction(expanded)
            return {"message": "Transaction rejected", "reason": "Insufficient balance"}

        # Append immediately as FRAUD_PENDING and hand off to the background worker.
        # The caller does not wait for the fraud model response.
        expanded["status"] = "FRAUD_PENDING"
        await append_transaction(expanded)
        await fraud_queue.put(expanded)

        return {
            "message": "Transaction queued for fraud assessment",
            "status": "FRAUD_PENDING",
            "tx_id": expanded["tx_id"],
        }

    except ValidationError:
        raise HTTPException(status_code=400, detail="Invalid transaction payload")
