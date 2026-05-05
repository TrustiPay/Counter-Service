from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ValidationError
import requests
import hashlib
import hmac
import json
import asyncio
import os
from secrets import compare_digest
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

tx_queue: asyncio.Queue = asyncio.Queue()

_db = None


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
    """Fetch PENDING_SYNC docs from Firebase and mark them Processing."""
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


def process_offline_transaction(tx: dict):
    """Validate and append a Firebase offline transaction to the central ledger."""
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
            append_transaction(payload)
            print(f"[Queue] {tx_id} rejected: invalid signature")
            return

        if check_duplicate_tx(tx_id):
            payload.update({"status": "REJECTED", "reason": "Duplicate transaction"})
            append_transaction(payload)
            print(f"[Queue] {tx_id} rejected: duplicate")
            return

        payload.update({"status": "PENDING", "reason": "Awaiting online processing"})
        append_transaction(payload)
        print(f"[Queue] {tx_id} appended to ledger")

    except Exception as e:
        print(f"[Queue] Error on {tx_id}: {e}")


# ── Async background loops ────────────────────────────────────────────────────

async def firebase_sync_loop():
    loop = asyncio.get_event_loop()
    print("[Firebase Sync] Started — interval: {SYNC_INTERVAL}s")
    while True:
        await asyncio.sleep(SYNC_INTERVAL)
        try:
            txs = await loop.run_in_executor(None, fetch_pending_transactions)
            for tx in txs:
                await tx_queue.put(tx)
        except Exception as e:
            print(f"[Firebase Sync] Error: {e}")


async def queue_worker_loop():
    loop = asyncio.get_event_loop()
    print("[Queue Worker] Started")
    while True:
        tx = await tx_queue.get()
        try:
            await loop.run_in_executor(None, process_offline_transaction, tx)
        finally:
            tx_queue.task_done()


# ── App lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_firebase()
    sync_task = asyncio.create_task(firebase_sync_loop())
    worker_task = asyncio.create_task(queue_worker_loop())
    yield
    sync_task.cancel()
    worker_task.cancel()
    await asyncio.gather(sync_task, worker_task, return_exceptions=True)


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
    KEY_MAP = {
        "t": "tx_id", "s": "sender_id", "r": "receiver_id",
        "ts": "timestamp", "a": "amount", "c": "category",
        "l": "location", "d": "device_id", "dt": "device_type",
        "nt": "network_type", "n": "nonce", "p": "prev_hash", "sig": "signature",
    }
    return {KEY_MAP[k]: v for k, v in data.dict().items()}


def verify_signature(payload: dict) -> bool:
    signature = payload.pop("signature", None)
    if not signature:
        return False
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    expected = hmac.new(HMAC_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    return compare_digest(expected, signature)


def check_duplicate_tx(tx_id: str) -> bool:
    r = requests.get(f"{CENTRAL_LEDGER_URL}/ledger/transactions/{tx_id}/exists")
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail="Central ledger service error")
    return r.json().get("exists", False)


def check_sufficient_balance(sender_id, amount: float) -> bool:
    r = requests.get(f"{CENTRAL_LEDGER_URL}/ledger/users/{sender_id}")
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail="Central ledger service error")
    return r.json().get("current_balance", 0) >= amount


def get_user(user_id) -> dict:
    r = requests.get(f"{CENTRAL_LEDGER_URL}/ledger/users/{user_id}")
    if r.status_code != 200:
        raise Exception("Error retrieving user details")
    return r.json()


def append_transaction(transaction: dict) -> dict:
    r = requests.post(f"{CENTRAL_LEDGER_URL}/ledger/transactions", json=transaction)
    if r.status_code != 200:
        raise Exception("Error appending transaction to ledger")
    return r.json()


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health_check():
    return {"status": "running", "queue_size": tx_queue.qsize()}


@app.get("/counter/queue/status")
def queue_status():
    return {"queue_size": tx_queue.qsize()}


@app.get("/ledger-data")
def get_ledger_data():
    return requests.get(f"{CENTRAL_LEDGER_URL}/ledger").json()


@app.post("/counter/transactions")
def process_transaction(iou: MinifiedIOU):
    try:
        expanded = expand_iou(iou)

        if expanded["amount"] <= 0:
            raise HTTPException(status_code=400, detail="Invalid transaction payload")

        if not verify_signature(expanded):
            expanded.update({"status": "REJECTED", "reason": "Invalid signature"})
            append_transaction(expanded)
            return {"message": "Transaction rejected", "reason": "Invalid signature"}

        if check_duplicate_tx(expanded["tx_id"]):
            expanded.update({"status": "REJECTED", "reason": "Duplicate transaction"})
            append_transaction(expanded)
            return {"message": "Transaction rejected", "reason": "Duplicate transaction"}

        if not check_sufficient_balance(expanded["sender_id"], expanded["amount"]):
            expanded.update({"status": "REJECTED", "reason": "Insufficient balance"})
            append_transaction(expanded)
            return {"message": "Transaction rejected", "reason": "Insufficient balance"}

        sender = get_user(expanded["sender_id"])
        receiver = get_user(expanded["receiver_id"])

        fraud_payload = {
            **expanded,
            "sender_current_balance": sender.get("current_balance"),
            "receiver_current_balance": receiver.get("current_balance"),
            "phone_number": sender.get("phone_number"),
        }

        fraud_response = requests.post(f"{FRAUD_MODEL_URL}/fraud/check", json=fraud_payload)
        if fraud_response.status_code != 200:
            raise HTTPException(status_code=500, detail="Fraud model service error")

        fraud_result = fraud_response.json()
        fraud_status = fraud_result.get("status", "REJECTED")
        expanded["reason"] = fraud_result.get("reason", "Fraud model error")

        if fraud_status == "ACCEPTED":
            expanded["status"] = "APPROVED"
        elif fraud_status == "OTP_PENDING":
            expanded["status"] = "OTP_PENDING"
        else:
            expanded["status"] = "REJECTED"

        append_transaction(expanded)
        return {
            "message": "Transaction processed successfully",
            "status": expanded["status"],
            "reason": expanded["reason"],
        }

    except ValidationError:
        raise HTTPException(status_code=400, detail="Invalid transaction payload")
