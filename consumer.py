from kafka import KafkaConsumer
import json

if __name__ == '__main__':
    consumer = KafkaConsumer(
        'registered_users',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='consumer_group-a'
    )
    print("starting the consumer")
    for msg in consumer:
        print(f"register user = {json.loads(msg.value)}")

