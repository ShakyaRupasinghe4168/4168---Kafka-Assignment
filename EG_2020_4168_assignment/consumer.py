import json
import random
from confluent_kafka import Consumer, Producer, KafkaException
import avro.schema
import avro.io
import io

with open('order.avsc', 'r') as schema_file:
    schema = avro.schema.parse(schema_file.read())

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest'
}

producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

consumer.subscribe(['orders', 'orders-retry'])

price_sum = 0.0
order_count = 0
retry_counts = {}  
MAX_RETRIES = 2

def deserialize_avro(binary_data, schema):
    bytes_reader = io.BytesIO(binary_data)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

def process_order(order):
    if random.random() < 0.2:
        raise Exception("Temporary processing error (simulated)")
    
    if random.random() < 0.05:
        raise ValueError("Permanent error: Invalid order data")
    
    print(f" --- Processed: Order {order['orderId']} - {order['product']} - ${order['price']:.2f}")
    return True

def calculate_running_average(price):
    global price_sum, order_count
    price_sum += price
    order_count += 1
    avg = price_sum / order_count
    print(f"  -> Running Average: ${avg:.2f} (from {order_count} orders)")
    return avg

def send_to_retry(key, value, current_topic):
    """Send failed message to retry queue"""
    print(f"  !! Sending to retry queue...")
    producer.produce(
        topic='orders-retry',
        key=key,
        value=value
    )
    producer.flush()

def send_to_dlq(key, value, error_msg):
    print(f"  X Sending to DLQ: {error_msg}")
    producer.produce(
        topic='orders-dlq',
        key=key,
        value=value
    )
    producer.flush()

def main():
    print("Starting Order Consumer...")
    print("Listening for messages (Ctrl+C to stop)\n")
    print("=" * 60)
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                raise KafkaException(msg.error())
            
            order = deserialize_avro(msg.value(), schema)
            order_id = order['orderId']
            current_topic = msg.topic()
            
            print(f" Received from {current_topic}: Order {order_id}")
            
            if order_id not in retry_counts:
                retry_counts[order_id] = 0
            
            try:
                process_order(order)
                
                calculate_running_average(order['price'])
                
                if order_id in retry_counts:
                    del retry_counts[order_id]
                
            except ValueError as e:
                send_to_dlq(msg.key(), msg.value(), str(e))
                
            except Exception as e:
                retry_counts[order_id] += 1
                
                if retry_counts[order_id] <= MAX_RETRIES:
                    print(f"   Temporary failure (attempt {retry_counts[order_id]}/{MAX_RETRIES}): {e}")
                    send_to_retry(msg.key(), msg.value(), current_topic)
                else:
                    send_to_dlq(
                        msg.key(), 
                        msg.value(), 
                        f"Max retries exceeded ({MAX_RETRIES})"
                    )
                    del retry_counts[order_id]
            
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    finally:
        consumer.close()
        print("Consumer stopped.")

if __name__ == '__main__':
    main()