# Real-Time Crypto Streamer

This project streams live cryptocurrency price data from Coinbase’s WebSocket API into a Kafka topic using a simple local Python producer. A local Python consumer subscribes to the same Kafka topic and writes the raw data into a local SQLite database. The Kafka broker is containerized using Docker, and AKHQ is included for monitoring and exploring Kafka topics in real time.

Architecture:

Local Python Producer  
         ↓  
    Kafka (Docker)  
         ↓  
Local Python Consumer  
         ↓  
     SQLite Database  


- The producer connects to Coinbase’s WebSocket feed and publishes live data to a Kafka topic (`coinbase_ticker`)  
- The consumer subscribes to the Kafka topic and writes raw messages to a local SQLite database (`coinbase.db`)  
- Kafka is run in a Docker container using Bitnami’s KRaft mode  
- AKHQ provides a web-based UI to view Kafka topics and inspect real-time messages 