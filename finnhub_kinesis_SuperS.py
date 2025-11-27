import os
import json
import time
import threading
import uuid

import boto3
from botocore.config import Config
from websocket import WebSocketApp

# ========= Konfiguration =========

AWS_REGION = "eu-central-1"
KINESIS_STREAM = "bdcloud01-kinesis01"

SUPER_SAMPLE_FACTOR = 10  # Supersampling-Faktor
BUFFER_SIZE = 1000  # ab wie vielen Records wir sofort flushen
FLUSH_INTERVAL_SECONDS = 1.0  # spätestens alle x Sekunden flushen
MAX_RECORDS_PER_BATCH = 500  # AWS-Limit für PutRecords

SYMBOLS = [
    "BINANCE:BTCUSDT",
    "BINANCE:ETHUSDT",
    "OANDA:EUR_USD",
]

BUFFER = []
BUFFER_LOCK = threading.Lock()

FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN")
if not FINNHUB_TOKEN:
    raise RuntimeError("FINNHUB_TOKEN ist nicht gesetzt (ENV Variable).")

WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}"

kinesis = boto3.client(
    "kinesis",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 5, "mode": "standard"}),
)

# ========= Hilfsfunktion: in Batches nach Kinesis schicken =========


def send_records(records, context="FLUSH"):
    """
    records: Liste von Dicts
    Schickt in Batches von höchstens 500 Records nach Kinesis.
    """
    total = len(records)
    if total == 0:
        return

    try:
        sent = 0
        failed_total = 0

        for start in range(0, total, MAX_RECORDS_PER_BATCH):
            batch = records[start : start + MAX_RECORDS_PER_BATCH]

            entries = [
                {
                    "Data": json.dumps(rec).encode("utf-8"),
                    "PartitionKey": rec.get("symbol", "unknown"),
                }
                for rec in batch
            ]

            resp = kinesis.put_records(Records=entries, StreamName=KINESIS_STREAM)
            failed = resp.get("FailedRecordCount", 0)
            failed_total += failed
            sent += len(batch)

        print(f"[{context}] {sent} Records → Kinesis, failed: {failed_total}")

    except Exception as e:
        print(f"[{context}-ERROR] Fehler beim Schreiben nach Kinesis: {e}")


# ========= Kinesis Buffer / periodischer Flush =========


def flush_buffer():
    """Schreibt den Buffer periodisch nach Kinesis (Hintergrund-Thread)."""
    global BUFFER
    while True:
        time.sleep(FLUSH_INTERVAL_SECONDS)

        with BUFFER_LOCK:
            if not BUFFER:
                continue
            records = BUFFER
            BUFFER = []

        send_records(records, context="FLUSH")


# ========= WebSocket Callbacks =========


def on_open(ws):
    print("[WS] Verbindung geöffnet. Sende subscribe Messages...")
    for sym in SYMBOLS:
        msg = {"type": "subscribe", "symbol": sym}
        ws.send(json.dumps(msg))
        print(f"[WS] Subscribed: {sym}")


def on_message(ws, message):
    global BUFFER
    try:
        payload = json.loads(message)
        if payload.get("type") != "trade":
            return

        trades = payload.get("data", [])
        now_ms = int(time.time() * 1000)

        with BUFFER_LOCK:
            for trade in trades:
                for i in range(SUPER_SAMPLE_FACTOR):
                    rec = {
                        "symbol": trade.get("s"),
                        "price": trade.get("p"),
                        "volume": trade.get("v"),
                        "trade_ts": trade.get("t"),
                        "ingest_ts": now_ms,
                        "sample_idx": i,
                        "sample_id": str(uuid.uuid4()),
                        "source": "finnhub_ws_trade_supersampled",
                    }
                    BUFFER.append(rec)

            # Sofort-Flush, wenn Buffer voll
            if len(BUFFER) >= BUFFER_SIZE:
                records = BUFFER
                BUFFER = []
                send_records(records, context="FLUSH-IMMEDIATE")

    except Exception as e:
        print(f"[WS-ERROR] Fehler beim Verarbeiten von Message: {e}")


def on_error(ws, error):
    print(f"[WS] Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[WS] Connection closed: code={close_status_code}, msg={close_msg}")


# ========= Main =========


def main():
    t = threading.Thread(target=flush_buffer, daemon=True)
    t.start()

    while True:
        ws = WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        try:
            print("[WS] Starte run_forever()...")
            ws.run_forever()
        except Exception as e:
            print(f"[WS] run_forever Fehler: {e}")

        print("[WS] Versuche Reconnect in 5 Sekunden...")
        time.sleep(5)


if __name__ == "__main__":
    main()
