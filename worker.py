import os
import json
import time
import redis
from kafka import KafkaConsumer

redis_host = os.environ.get('REDIS_HOST', 'redis')
redis_port = int(os.environ.get('REDIS_PORT', 6379))
broker_url = os.environ.get('REDPANDA_BROKER', 'redpanda:9092')
topic_name = os.environ.get('TOPIC_NAME', 'visits-topic')

r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

print(f"[*] Starting Background Worker... Connecting to Redpanda at {broker_url}", flush=True)

consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=[broker_url],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='visits-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"[*] Successfully connected to Redpanda! Listening to '{topic_name}'...", flush=True)
    except Exception as e:
        print(f"[!] Waiting for Redpanda broker... ({e})", flush=True)
        time.sleep(3)

for message in consumer:
    event = message.value
    container_id = event.get('container_id', 'unknown')
    
    global_val = r.incr('global_counter')
    node_val = r.incr(f'node_counter:{container_id}')
    
    print(f"[+] Event Processed | Node: {container_id} (Visits: {node_val}) | Global Visits: {global_val}", flush=True)
