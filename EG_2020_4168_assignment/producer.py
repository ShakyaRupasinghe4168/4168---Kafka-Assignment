import json
import random
import time
from confluent_kafka import Producer
import avro.schema
import avro.io
import io

with open('order.avsc', 'r') as schema_file:
    schema = avro.schema.parse(schema_file.read())

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'order-producer'
}

producer = Producer(conf)

def serialize_avro(order_data, schema):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(order_data, encoder)
    return bytes_writer.getvalue()

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_order(order_id):
    products = ['Smartphone', 'Tablet', 'Smartwatch', 'Printer', 'Router', 'Laptop']
    return {
        'orderId': str(order_id),
        'product': random.choice(products),
        'price': round(random.uniform(1000.0, 5000.0), 2)
    }

def main():
    print("Starting Order Producer...")
    print("Press Ctrl+C to stop\n")
    
    order_id = 100
    
    try:
        while True:
            order = generate_order(order_id)
            print(f"Producing: {order}")
            
            serialized_order = serialize_avro(order, schema)
            
            producer.produce(
                topic='orders',
                key=str(order_id),
                value=serialized_order,
                callback=delivery_report
            )
            
            producer.poll(0)
            
            order_id += 1
            time.sleep(4)  
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        print("Producer stopped.")

if __name__ == '__main__':
    main()