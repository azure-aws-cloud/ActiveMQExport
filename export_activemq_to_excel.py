import json
import time
import stomp
import pandas as pd
from threading import Event

# Load configuration
with open('config.json') as config_file:
    config = json.load(config_file)

HOST = config['host']
PORT = config['port']
USERNAME = config['username']
PASSWORD = config['password']
QUEUE_NAME = config['queue_name']
OUTPUT_EXCEL = config['output_excel']
READ_TIMEOUT = config['read_timeout']

# Listener class to collect messages
class MessageCollector(stomp.ConnectionListener):
    def __init__(self):
        self.messages = []
        self.done = Event()

    def on_message(self, frame):
        print(f"Received message: {frame.body}")
        self.messages.append(frame.body)

    def on_disconnected(self):
        print("Disconnected from ActiveMQ.")
        self.done.set()

# Connect to ActiveMQ
conn = stomp.Connection([(HOST, PORT)])
listener = MessageCollector()
conn.set_listener('', listener)
conn.start()
conn.connect(USERNAME, PASSWORD, wait=True)

print(f"Subscribed to queue: {QUEUE_NAME}")
conn.subscribe(destination=QUEUE_NAME, id=1, ack='auto')

# Wait to collect messages
print(f"Waiting {READ_TIMEOUT} seconds to collect messages...")
time.sleep(READ_TIMEOUT)

conn.disconnect()

# Process and export messages
if not listener.messages:
    print("No messages received.")
else:
    try:
        data = [json.loads(msg) for msg in listener.messages]
        df = pd.DataFrame(data)
        df.to_excel(OUTPUT_EXCEL, index=False)
        print(f"Exported {len(data)} messages to {OUTPUT_EXCEL}")
    except json.JSONDecodeError:
        print("Error: Some messages are not valid JSON. Please check message format.")
