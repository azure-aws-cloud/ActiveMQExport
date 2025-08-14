import json
import stomp
import pandas as pd
import time

# --- Configurable Parameters ---
HOST = 'localhost'
PORT = 61613
USERNAME = 'admin'
PASSWORD = 'admin'
QUEUE_NAME = '/queue/my_queue'
OUTPUT_EXCEL = 'activemq_messages.xlsx'
MAX_MESSAGES = 1000        # Set a high number or loop until empty
RECEIVE_TIMEOUT = 2        # Seconds to wait for a message (per iteration)

# --- Connect to ActiveMQ ---
conn = stomp.Connection12([(HOST, PORT)])
conn.connect(USERNAME, PASSWORD, wait=True)
conn.subscribe(destination=QUEUE_NAME, id=1, ack='client-individual')

print(f"Connected to ActiveMQ queue: {QUEUE_NAME}")
messages = []

# --- Pull messages synchronously ---
for i in range(MAX_MESSAGES):
    frame = conn.receive_frame(timeout=RECEIVE_TIMEOUT)
    if frame is None or frame.command != 'MESSAGE':
        print("No more messages or timed out.")
        break
    print(f"Received message {i+1}")
    messages.append(frame.body)
    conn.ack(frame.headers['message-id'], subscription=frame.headers['subscription'])

conn.disconnect()
print(f"Disconnected. Total messages received: {len(messages)}")

# --- Export to Excel ---
if messages:
    try:
        data = [json.loads(msg) for msg in messages]
        df = pd.DataFrame(data)
        df.to_excel(OUTPUT_EXCEL, index=False)
        print(f"Exported messages to: {OUTPUT_EXCEL}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
else:
    print("No messages to export.")
