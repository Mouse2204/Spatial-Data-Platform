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

def generate_wkt(feature_type):
    lon, lat = round(random.uniform(106.6, 106.8), 6), round(random.uniform(10.7, 10.9), 6)
    
    if feature_type == "POINT":
        return f"POINT({lon} {lat})"
    
    elif feature_type == "LINESTRING":
        return f"LINESTRING({lon} {lat}, {lon+0.01} {lat+0.01}, {lon+0.02} {lat})"
    
    elif feature_type == "POLYGON":
        d = 0.01
        return f"POLYGON(({lon} {lat}, {lon+d} {lat}, {lon+d} {lat+d}, {lon} {lat+d}, {lon} {lat}))"
    
    elif feature_type == "MULTIPOLYGON":
        return f"MULTIPOLYGON((({lon} {lat}, {lon+0.005} {lat}, {lon+0.005} {lat+0.005}, {lon} {lat+0.005}, {lon} {lat})), (({lon+0.02} {lat+0.02}, {lon+0.025} {lat+0.02}, {lon+0.025} {lat+0.025}, {lon+0.02} {lat+0.025}, {lon+0.02} {lat+0.02})))"

def main():
    conf = {
        'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292',
        'client.id': 'spatial-producer-python'
    }

    producer = Producer(conf)
    topic = 'spatial-events'
    
    features = [
        {"id": "V001", "type": "POINT"},
        {"id": "R_01", "type": "LINESTRING"},
        {"id": "Z_99", "type": "POLYGON"},
        {"id": "M_10", "type": "MULTIPOLYGON"}
    ]

    try:
        while True:
            for f in features:
                data = {
                    "feature_id": f["id"],
                    "feature_type": f["type"],
                    "wkt_data": generate_wkt(f["type"]),
                    "timestamp": datetime.now().isoformat()
                }
                
                producer.produce(
                    topic, 
                    key=f["id"], 
                    value=json.dumps(data), 
                    callback=delivery_report
                )
            
            producer.poll(0)
            time.sleep(5) 
            
    except KeyboardInterrupt:
        print("Dừng Producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()