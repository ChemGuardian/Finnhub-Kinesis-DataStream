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
KINESIS_STREAM = "bdcloud01-kinesis01"  # Stream-Name

SUPER_SAMPLE_FACTOR = 1000  # Faktor fürs Supersampling

# Symbole, die du abonnieren willst
SYMBOLS = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "OANDA:EUR_USD"]
# nach Bedarf anpassen

# Buffer für batched Writes nach Kinesis
BUFFER = []
BUFFER_SIZE = 2000  # ab wie vielen Records sofort geflusht wird
FLUSH_INTERVAL_SECONDS = 1.0  # spätestens alle x Sekunden flushen
BUFFER_LOCK = threading.Lock()

FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN")
if not FINNHUB_TOKEN:
    raise RuntimeError("FINNHUB_TOKEN ist nicht gesetzt (ENV Variable).")

WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}"

# Kinesis Client
kinesis = boto3.client(
    "kinesis",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 5, "mode": "standard"}),
)

# ========= Kinesis Buffer / Flush =========


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

        entries = [
            {
                "Data": json.dumps(rec).encode("utf-8"),
                "PartitionKey": rec.get("symbol", "unknown"),
            }
            for rec in records
        ]

        try:
            resp = kinesis.put_records(Records=entries, StreamName=KINESIS_STREAM)
            failed = resp.get("FailedRecordCount", 0)
            print(f"[FLUSH] {len(records)} Records → Kinesis, failed: {failed}")
        except Exception as e:
            print(f"[FLUSH-ERROR] Fehler beim Schreiben nach Kinesis: {e}")


# ========= WebSocket Callbacks =========


def on_open(ws):
    print("[WS] Verbindung geöffnet. Sende subscribe Messages...")
    for sym in SYMBOLS:
        msg = {"type": "subscribe", "symbol": sym}
        ws.send(json.dumps(msg))
        print(f"[WS] Subscribed: {sym}")


def on_message(ws, message):
    """
    Erwartete Finnhub-Message:
    {
      "type": "trade",
      "data": [
        { "p": 261.74, "s": "AAPL", "t": 1582641634534, "v": 100 },
        ...
      ]
    }
    """
    global BUFFER
    try:
        payload = json.loads(message)
        if payload.get("type") != "trade":
            return

        trades = payload.get("data", [])
        now_ms = int(time.time() * 1000)

        with BUFFER_LOCK:
            for trade in trades:
                # Supersampling: aus jedem echten Trade SUPER_SAMPLE_FACTOR Records machen
                for i in range(SUPER_SAMPLE_FACTOR):
                    rec = {
                        "symbol": trade.get("s"),
                        "price": trade.get("p"),
                        "volume": trade.get("v"),
                        "trade_ts": trade.get("t"),  # Originalzeitpunkt des Trades (ms)
                        "ingest_ts": now_ms,  # Zeitpunkt der Verarbeitung
                        "sample_idx": i,  # Index dieses Samples
                        "sample_id": str(uuid.uuid4()),  # eindeutige ID pro Sample
                        "source": "finnhub_ws_trade_supersampled",
                    }
                    BUFFER.append(rec)

            # Sofort-Flush, wenn Buffer voll
            if len(BUFFER) >= BUFFER_SIZE:
                records = BUFFER
                BUFFER = []

                entries = [
                    {
                        "Data": json.dumps(r).encode("utf-8"),
                        "PartitionKey": r.get("symbol", "unknown"),
                    }
                    for r in records
                ]
                try:
                    resp = kinesis.put_records(
                        Records=entries, StreamName=KINESIS_STREAM
                    )
                    failed = resp.get("FailedRecordCount", 0)
                    print(
                        f"[FLUSH-IMMEDIATE] {len(records)} Records → Kinesis, failed: {failed}"
                    )
                except Exception as e:
                    print(f"[FLUSH-IMMEDIATE-ERROR] {e}")

    except Exception as e:
        print(f"[WS-ERROR] Fehler beim Verarbeiten von Message: {e}")


def on_error(ws, error):
    print(f"[WS] Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[WS] Connection closed: code={close_status_code}, msg={close_msg}")


# ========= Main =========


def main():
    # Hintergrund-Thread zum periodischen Flush
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
