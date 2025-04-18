import json
import sqlite3
from confluent_kafka import Consumer

# Kafka config
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'coinbase-sqlite-consumer',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['coinbase_ticker'])

# SQLite setup
conn = sqlite3.connect("coinbase.db")
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS ticker_data (
    product_id TEXT,
    price REAL,
    volume_24h REAL,
    time TEXT
)
''')
conn.commit()

print("✅ Listening for messages on 'coinbase_ticker'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"⚠️ Kafka error: {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            if data.get("type") == "ticker":
                cursor.execute('''
                    INSERT INTO ticker_data (product_id, price, volume_24h, time)
                    VALUES (?, ?, ?, ?)
                ''', (
                    data.get("product_id"),
                    float(data.get("price", 0)),
                    float(data.get("volume_24h", 0)),
                    data.get("time")
                ))
                conn.commit()
                print(f"💾 Saved {data['product_id']} @ {data['price']}")
        except Exception as e:
            print(f"❌ Failed to process message: {e}")

except KeyboardInterrupt:
    print("⏹️  Consumer stopped.")

finally:
    consumer.close()
    conn.close()