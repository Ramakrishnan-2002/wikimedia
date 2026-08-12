from confluent_kafka import Producer
import json
import requests
import sseclient
from confluent_kafka import AdminClient

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

HEADERS ={ "User-Agent": "KafkaLearningProject/1.0"
          "(https://github.com/Ramakrishnan-2002/wikimedia.git; "
        "vmrammani@gmail.com)"}


#creating Kafka Topic

def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
    existing_topics = admin_client.list_topics(timeout=10).topics #wait atmost 10 seconds for kafka to respond
    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' already exists.")
        return
    futures = admin_client.create_topics([{"topic": topic_name, "num_partitions": 3, "replication_factor": 1}])
    try:
        futures[topic_name].result()  # Wait for the topic creation to complete
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")


#delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


#Wikimedia stream producer
def stream_wikimedia(producer):
    print("\nConnecting to Wikimedia stream...")
    response =requests.get(WIKIMEDIA_STREAM_URL, stream=True, headers=HEADERS, timeout=10)
    response.raise_for_status()  # Raise an exception for HTTP errors
    client = sseclient.SSEClient(response) #separate the stream into events and data
    print("Connected to Wikimedia stream. Listening for events...")
    print("Producing events to Kafka topic 'wikimedia.recentchange'...")
    print("Press Ctrl+C to stop the producer.")
    for event in client.events():
        if event.data is None:
            continue
        try:
            change_event = json.loads(event.data)
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
            continue
        producer.produce("wikimedia.recentchange", key=str(change_event.get("id")), value=json.dumps(change_event), callback=delivery_report)
        producer.poll(0)  # Trigger delivery report callbacks
        print(f"Sent: "
              f"{change_event.get('type','N/A')} | "
              f"{change_event.get('title','N/A')}")


def main():
    print("Starting Wikimedia stream producer...")
    create_topic("wikimedia.recentchange")
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    try:
        stream_wikimedia(producer)
    except KeyboardInterrupt:   
        print("\nProducer stopped by user.")
    except Exception as e:  
        print(f"\nAn error occurred: {e}")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting
        print("Producer flushed and exiting.")

if __name__ == "__main__":
    main()
