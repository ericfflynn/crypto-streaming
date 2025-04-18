import asyncio
import json
import websockets
from confluent_kafka import Producer

# Kafka config
producer = Producer({'bootstrap.servers': 'localhost:9092'})

def send_to_kafka(topic, message):
    producer.produce(topic, json.dumps(message).encode('utf-8'))
    producer.flush()

async def stream_from_coinbase():
    uri = "wss://ws-feed.exchange.coinbase.com"
    subscribe_msg = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD"]}]
    }

    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps(subscribe_msg))
        print("Connected to Coinbase stream...")

        while True:
            try:
                raw_msg = await ws.recv()
                data = json.loads(raw_msg)

                if data.get("type") == "ticker":
                    send_to_kafka("coinbase_ticker", data)
                    print(f"{data['product_id']} | {data['price']} sent to Kafka")
            except Exception as e:
                print(f"WebSocket error: {e}")
                break

asyncio.run(stream_from_coinbase())