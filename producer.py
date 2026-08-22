from confluent_kafka import Producer
import json
import requests
import sseclient
from confluent_kafka.admin import AdminClient, NewTopic ,ConfigResource

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

HEADERS ={ "User-Agent": "KafkaLearningProject/1.0"
          "(https://github.com/Ramakrishnan-2002/wikimedia.git; "
        "vmrammani@gmail.com)"}


#creating Kafka Topic

def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
    existing_topics = admin_client.list_topics(timeout=10).topics #wait atmost 10 seconds for kafka to respond
    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' already exists. Updating min.insync.replicas...")
        resource = ConfigResource(
            ConfigResource.Type.TOPIC,
            topic_name,
            set_config={"min.insync.replicas": "2"}   # enforce durability
        )
        futures = admin_client.alter_configs([resource])
        for res, f in futures.items():
            try:
                f.result()
                print(f"Updated {res.name} with min.insync.replicas=2")
            except Exception as e:
                print(f"Failed to update {res.name}: {e}")
        return
    futures = admin_client.create_topics([NewTopic(topic=topic_name, num_partitions=3, replication_factor=1,
                                                   config={"min.insync.replicas": "2"})]) #set at creation
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
    producer = Producer({"bootstrap.servers": "localhost:9092",
                        "acks":"all", #wait for all in-sync replicas
                        "enable.idempotence": True,# optional: ensures exactly-once delivery
                        "retries": 2147483647,        # INT_MAX: Keep retrying until timeout
                        "retry.backoff.ms": 500,       # Wait 500ms between retry attempts
                        "delivery.timeout.ms": 60000,   # Fail if not acknowledged within 60 seconds
                        "compression.type": "lz4",                 # efficient compression
                        "linger.ms": 5,                            # wait 5ms to batch
                        "batch.size": 32768,                       # 32 KB batch size
                        "max.in.flight.requests.per.connection": 5 # preserve ordering with retries
            })
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
