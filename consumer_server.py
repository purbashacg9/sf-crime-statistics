from kafka import KafkaConsumer
import json

if __name__ == "__main__":
    cs = KafkaConsumer(
        "org.udacity.assignment.crime-statistics", 
        bootstrap_servers="localhost:9092",
        group_id='consumer-server-plain',
    )
    for message in cs:        
        print ("%s:%d:%d: " % (
            message.topic, 
            message.partition,
            message.offset
        )) 
        if message.key: 
            print(f"key = {message.key.decode('utf-8')}")  

        print(f"value = {message.value.decode('utf-8')}")    
