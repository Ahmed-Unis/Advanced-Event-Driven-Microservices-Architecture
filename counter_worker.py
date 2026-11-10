import os
import json
import time
import redis
from kafka import KafkaConsumer, KafkaProducer

redis_host = os.environ.get('REDIS_HOST', 'redis')
redis_port = int(os.environ.get('REDIS_PORT', 6379))
brokers = os.environ.get('REDPANDA_BROKERS', 'redpanda-1:9092,redpanda-2:9092,redpanda-3:9092').split(',')
topic_name = os.environ.get('TOPIC_NAME', 'visits-topic')
dlq_topic = os.environ.get('DLQ_TOPIC', 'visits-dlq')

r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

print(f"[*] Starting Counter Worker (counter-group)... Connecting to {brokers}", flush=True)

consumer = None
dlq_producer = None

while consumer is None:
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=brokers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='counter-group'
        )
        dlq_producer = KafkaProducer(bootstrap_servers=brokers)
        print(f"[*] Counter Worker connected! Listening to '{topic_name}'...", flush=True)
    except Exception as e:
        print(f"[!] Waiting for Redpanda cluster... ({e})", flush=True)
        time.sleep(3)

for message in consumer:
    try:
        raw_payload = message.value.decode('utf-8')
        event = json.loads(raw_payload)
        
        if not isinstance(event, dict) or 'container_id' not in event or not event['container_id']:
            raise ValueError("Missing or empty 'container_id' field")
            
        container_id = event['container_id']
        
        global_val = r.incr('global_counter')
        node_val = r.incr(f'node_counter:{container_id}')
        
        print(f"[COUNTER] Partition: {message.partition} | Offset: {message.offset} | Node: {container_id} (Visits: {node_val}) | Global: {global_val}", flush=True)
        
    except Exception as e:
        print(f"[DLQ ALERT] Malformed payload detected at Partition {message.partition}, Offset {message.offset}! Error: {e}. Routing to '{dlq_topic}'...", flush=True)
        try:
            dlq_producer.send(dlq_topic, key=message.key, value=message.value)
            dlq_producer.flush()
        except Exception as dlq_err:
            print(f"[DLQ ERROR] Failed to route message to DLQ: {dlq_err}", flush=True)
