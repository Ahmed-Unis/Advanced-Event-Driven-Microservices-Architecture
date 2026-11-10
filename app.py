from flask import Flask, Response, request
import redis
import socket
import os
import json
import time
from kafka import KafkaProducer

app = Flask(__name__)

redis_host = os.environ.get('REDIS_HOST', 'redis')
redis_port = int(os.environ.get('REDIS_PORT', 6379))
r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

brokers = os.environ.get('REDPANDA_BROKERS', 'redpanda-1:9092,redpanda-2:9092,redpanda-3:9092').split(',')
topic_name = os.environ.get('TOPIC_NAME', 'visits-topic')

producer = None

def get_producer():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=brokers,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=5
        )
    return producer

@app.route('/')
def index():
    global producer
    container_id = socket.gethostname()
    
    try:
        prod = get_producer()
        event_data = {'container_id': container_id}
        prod.send(topic_name, key=container_id, value=event_data)
        prod.flush(timeout=2)
    except Exception as e:
        app.logger.error(f"Failed to publish event: {e}")
        producer = None  

    global_count = r.get('global_counter') or 0
    node_count = r.get(f'node_counter:{container_id}') or 0
    
    html_response = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Lab 3: Advanced EDA & SSE</title>
        <style>
            body {{ font-family: Arial, sans-serif; background-color: #f4f6f9; text-align: center; padding-top: 50px; }}
            .card {{ background: white; padding: 30px; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); display: inline-block; min-width: 380px; }}
            h2 {{ color: #2c3e50; margin-bottom: 5px; }}
            .subtitle {{ color: #7f8c8d; font-size: 0.9em; margin-bottom: 20px; }}
            .stat {{ font-size: 1.2em; margin: 15px 0; padding: 10px; background: #eef2f5; border-radius: 5px; }}
            .highlight {{ color: #e74c3c; font-weight: bold; font-size: 1.3em; }}
            .node-id {{ color: #2980b9; font-family: monospace; font-weight: bold; }}
            .live-badge {{ background-color: #27ae60; color: white; padding: 3px 8px; border-radius: 12px; font-size: 0.7em; vertical-align: middle; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h2>Advanced EDA <span class="live-badge">LIVE SSE</span></h2>
            <div class="subtitle">CQRS, Key-Based Partitioning & HA Cluster</div>
            <div class="stat">
                Container ID (Hostname): <br>
                <span class="node-id" id="node-id">{container_id}</span>
            </div>
            <div class="stat">
                Global Visits: <br>
                <span class="highlight" id="global-count">{global_count}</span>
            </div>
            <div class="stat">
                Node Visits: <br>
                <span class="highlight" id="node-count">{node_count}</span>
            </div>
        </div>
        <script>
            const currentNode = "{container_id}";
            const source = new EventSource("/stream?node=" + currentNode);
            source.onmessage = function(event) {{
                const data = JSON.parse(event.data);
                document.getElementById("global-count").innerText = data.global_count;
                document.getElementById("node-count").innerText = data.node_count;
            }};
        </script>
    </body>
    </html>
    """
    return html_response

@app.route('/stream')
def stream():
    target_node = request.args.get('node', socket.gethostname())
    def event_stream():
        while True:
            g_count = r.get('global_counter') or 0
            n_count = r.get(f'node_counter:{target_node}') or 0
            data = json.dumps({'global_count': g_count, 'node_count': n_count})
            yield f"data: {data}\n\n"
            time.sleep(0.5)
            
    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)
