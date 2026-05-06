# Counter Service

The Counter Service is the transaction entry point for the TrustiPay backend. It validates incoming payments from mobile clients (both online and offline/Firebase), queues them for asynchronous fraud assessment, and forwards approved transactions to the Central Ledger Service.

---

## Architecture

```
Mobile Client
     │
     ▼
Counter Service  (:8000)
     │
     ├── Signature / duplicate / balance checks
     │
     ├── Appends transaction as FRAUD_PENDING
     │        │
     │        ▼
     │   fraud_queue  ──►  Fraud Worker (one at a time)
     │                          │
     │                          ▼
     │                    Fraud Model Service (:8002)
     │                          │
     │                          ▼
     │                    PATCH → Central Ledger (:8001)
     │
     └── Firebase Sync Loop
              │
              ▼
         Offline transactions → same fraud queue
```

---

## Services

| Service | Default URL | Notes |
|---|---|---|
| Counter Service | `http://localhost:8000` | This service |
| Central Ledger Service | `http://central-ledger-service:8001` | Stores all transactions |
| Fraud Model Service | `http://fraud-model-service:8002` | ML fraud detection |

---

## Environment Variables

Copy `.env.example` to `.env` and fill in the values.

| Variable | Default | Description |
|---|---|---|
| `COUNTER_URL` | `https://counter.trustipay.online` | Public URL of this service |
| `CENTRAL_LEDGER_URL` | `http://central-ledger-service:8001` | Central ledger base URL |
| `FRAUD_MODEL_URL` | `http://fraud-model-service:8002` | Fraud model base URL |
| `FIREBASE_CREDENTIALS_JSON` | — | Firebase service account JSON (stringified) |
| `SYNC_INTERVAL` | `5` | Seconds between Firebase offline sync polls |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Service health and offline TX queue size |
| `GET` | `/counter/status` | Live queue depths and active fraud check details |
| `GET` | `/counter/queue/status` | Offline TX queue size |
| `GET` | `/ledger-data` | Proxy — returns all transactions from the ledger |
| `POST` | `/counter/transactions` | Submit a new transaction (minified IOU format) |

### `POST /counter/transactions`

Accepts a minified IOU payload and returns immediately. The fraud check runs in the background.

**Request body**

| Field | Type | Description |
|---|---|---|
| `t` | string | Transaction ID |
| `s` | int | Sender user ID |
| `r` | int | Receiver user ID |
| `ts` | string | Timestamp |
| `a` | float | Amount |
| `c` | string | Category |
| `l` | string | Location |
| `d` | string | Device ID |
| `dt` | string | Device type |
| `nt` | string | Network type |
| `n` | string | Nonce |
| `p` | string | Previous hash |
| `sig` | string | HMAC-SHA256 signature |

**Response**

```json
{
  "message": "Transaction queued for fraud assessment",
  "status": "FRAUD_PENDING",
  "tx_id": "<tx_id>"
}
```

### Transaction statuses

| Status | Meaning |
|---|---|
| `FRAUD_PENDING` | Passed validation, waiting for fraud model |
| `APPROVED` | Fraud model accepted the transaction |
| `OTP_PENDING` | Fraud model requires OTP confirmation |
| `REJECTED` | Failed signature, duplicate, balance, or fraud check |
| `PENDING` | Offline transaction awaiting online processing |

---

## Background Workers

Three background tasks run for the lifetime of the process:

**Firebase Sync Loop** — polls Firestore every `SYNC_INTERVAL` seconds for `PENDING_SYNC` documents, marks them `Processing`, and pushes them into the offline TX queue.

**Queue Worker** — drains the offline TX queue, validates each transaction (signature, duplicate), and hands valid ones to the fraud queue.

**Fraud Worker** — processes the fraud queue **one transaction at a time** to avoid overloading the fraud model service. Fetches sender and receiver details in parallel, calls the fraud model, then PATCHes the Central Ledger with the final status.

---

## Running the Service

### Local

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in credentials
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Docker

```bash
docker build -t counter-service .
docker run -p 8000:8000 --env-file .env counter-service
```

---

## Dashboard

A Streamlit dashboard is included in the `dashboard/` directory. It shows live queue depths, the currently active fraud check, and a searchable, colour-coded ledger transactions table.

### Local

```bash
pip install streamlit pandas requests
streamlit run dashboard/dashboard.py
```

Open `http://localhost:8501`. Set the Counter Service URL in the sidebar if it differs from `http://localhost:8000`.

### Docker

```bash
docker build -t counter-dashboard ./dashboard
docker run -p 8501:8501 -e COUNTER_URL=http://host.docker.internal:8000 counter-dashboard
```

This uses the separate `dashboard/Dockerfile` and runs only the Streamlit dashboard on port `8501`.

If your deploy platform builds from the repository root, use the root-context dashboard Dockerfile instead:

```bash
docker build -t counter-dashboard -f Dockerfile.dashboard .
docker run -p 8501:8501 -e COUNTER_URL=http://host.docker.internal:8000 counter-dashboard
```

For hosted dashboard deployments, make sure the Dockerfile is `Dockerfile.dashboard` or `dashboard/Dockerfile`, the exposed port is `8501`, and there is no start command such as `uvicorn main:app`.

---

## Project Structure

```
counter-service/
├── main.py                 # FastAPI app
├── requirements.txt
├── Dockerfile              # FastAPI API image
├── Dockerfile.dashboard    # Streamlit dashboard image for root-context builds
├── .env.example
├── .gitignore
├── .dockerignore
└── dashboard/
    ├── .dockerignore
    ├── dashboard.py        # Streamlit dashboard
    ├── requirements.txt
    └── Dockerfile
```
