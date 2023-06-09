import json
import websocket
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('utf-8'))

def on_message(ws, message):
    data = json.loads(message)
    print(data)
    producer.send('wiki_stream', value=data)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    print("### opened ###")


websocket.enableTrace(True)
ws = websocket.WebSocketApp(
    "https://stream.wikimedia.org/v2/stream/page-create",
    on_message = on_message,
    on_error = on_error,
    on_close = on_close
)
ws.on_open = on_open
ws.run_forever()


