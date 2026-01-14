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

def generate_wkt_3d(feature_type):
    lon, lat = round(random.uniform(106.6, 106.8), 6), round(random.uniform(10.7, 10.9), 6)
    z = round(random.uniform(0, 500), 2)
    
    if feature_type == "POINT":
        return f"POINT Z ({lon} {lat} {z})"
    
    elif feature_type == "LINESTRING":
        return f"LINESTRING Z ({lon} {lat} {z}, {lon+0.01} {lat+0.01} {z+20.5}, {lon+0.02} {lat} {z+40.0})"
    
    elif feature_type == "POLYGON":
        d = 0.01
        return f"POLYGON Z (({lon} {lat} {z}, {lon+d} {lat} {z}, {lon+d} {lat+d} {z}, {lon} {lat+d} {z}, {lon} {lat} {z}))"
    
    elif feature_type == "MULTIPOLYGON":
        z2 = z + 50.0
        return f"MULTIPOLYGON Z ((({lon} {lat} {z}, {lon+0.005} {lat} {z}, {lon+0.005} {lat+0.005} {z}, {lon} {lat+0.005} {z}, {lon} {lat} {z})), (({lon+0.02} {lat+0.02} {z2}, {lon+0.025} {lat+0.02} {z2}, {lon+0.025} {lat+0.025} {z2}, {lon+0.02} {lat+0.025} {z2}, {lon+0.02} {lat+0.02} {z2})))"

def main():
    conf = {
        'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292',
        'client.id': 'spatial-producer-python'
    }

    producer = Producer(conf)
    topic = 'spatial-events'
    
    features = [
        {"id": "V001_AIR", "type": "POINT"},
        {"id": "R_01_PATH", "type": "LINESTRING"},
        {"id": "Z_99_AREA", "type": "POLYGON"},
        {"id": "M_10_COMPLEX", "type": "MULTIPOLYGON"}
    ]

    try:
        print("=== PRODUCER 3D IS RUNNING ===")
        while True:
            for f in features:
                data = {
                    "feature_id": f["id"],
                    "feature_type": f["type"],
                    "wkt_data": generate_wkt_3d(f["type"]),
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
        pass
    finally:
        producer.flush()

if __name__ == "__main__":
    main()