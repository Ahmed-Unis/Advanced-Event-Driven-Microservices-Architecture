import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer

brokers = os.environ.get('REDPANDA_BROKERS', 'redpanda-1:9092,redpanda-2:9092,redpanda-3:9092').split(',')
topic_name = os.environ.get('TOPIC_NAME', 'visits-topic')

print(f"[*] Starting Audit Worker (audit-group)... Connecting to {brokers}", flush=True)

consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=brokers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='audit-group'
        )
        print(f"[*] Audit Worker connected! Listening to '{topic_name}'...", flush=True)
    except Exception as e:
        print(f"[!] Waiting for Redpanda cluster... ({e})", flush=True)
        time.sleep(3)

for message in consumer:
    ts = datetime.fromtimestamp(message.timestamp / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    try:
        event = json.loads(message.value.decode('utf-8'))
        container_id = event.get('container_id', 'MALFORMED_MISSING_ID') if isinstance(event, dict) else 'MALFORMED_NOT_DICT'
    except Exception:
        container_id = 'MALFORMED_NON_JSON'
        
    log_entry = f"[AUDIT LOG] Timestamp: {ts} | Container_ID: {container_id} | Partition: {message.partition} | Offset: {message.offset}"
    print(log_entry, flush=True)
    
    with open("audit.log", "a") as log_file:
        log_file.write(log_entry + "\n")
