import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    conf = {
        'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292',
        'client.id': 'spatial-producer-python'
    }

    producer = Producer(conf)
    topic = 'spatial-events'
    vehicles = ['V001', 'V002', 'V003', 'V004', 'V005']

    try:
        while True:
            for vehicle_id in vehicles:
                data = {
                    "vehicle_id": vehicle_id,
                    "lat": round(random.uniform(10.7, 10.9), 6),
                    "lon": round(random.uniform(106.6, 106.8), 6),
                    "timestamp": datetime.now().isoformat()
                }
                
                producer.produce(
                    topic, 
                    key=vehicle_id, 
                    value=json.dumps(data), 
                    callback=delivery_report
                )
                
            producer.poll(0)
            time.sleep(2)
            
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()

if __name__ == "__main__":
    main()