import json
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
import requests
import sseclient

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {
    "User-Agent": "KafkaLearningProject/1.0 (https://github.com/Ramakrishnan-2002/wikimedia.git; vmrammani@gmail.com)"
}


def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
    existing_topics = admin_client.list_topics(timeout=10).topics

    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' already exists. Updating min.insync.replicas...")
        resource = ConfigResource(
            ConfigResource.Type.TOPIC,
            topic_name,
            set_config={"min.insync.replicas": "1"},  # Set to 1 for single-node local cluster
        )
        futures = admin_client.alter_configs([resource])
        for res, f in futures.items():
            try:
                f.result()
                print(f"Updated {res.name} with min.insync.replicas=1")
            except Exception as e:
                print(f"Failed to update {res.name}: {e}")
        return

    futures = admin_client.create_topics([
        NewTopic(
            topic=topic_name,
            num_partitions=3,
            replication_factor=1,
            config={"min.insync.replicas": "1"},  # Must be <= replication_factor
        )
    ])
    try:
        futures[topic_name].result()
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def stream_wikimedia(producer):
    print("\nConnecting to Wikimedia stream...")
    response = requests.get(WIKIMEDIA_STREAM_URL, stream=True, headers=HEADERS, timeout=10)
    response.raise_for_status()

    client = sseclient.SSEClient(response)
    print("Connected! Streaming to 'wikimedia.recentchange' (Press Ctrl+C to stop)...")

    for event in client.events():
        if not event.data:
            continue
        try:
            change_event = json.loads(event.data)
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
            continue

        # Handle buffer backpressure smoothly
        while True:
            try:
                producer.produce(
                    "wikimedia.recentchange",
                    key=str(change_event.get("id")),
                    value=json.dumps(change_event),
                    callback=delivery_report,
                )
                break  # Successfully queued
            except BufferError:
                # Local buffer is full: poll to process background deliveries and free up space
                producer.poll(0.1)

        producer.poll(0)  # Serve delivery callbacks asynchronously
        print(f"Sent: {change_event.get('type', 'N/A')} | {change_event.get('title', 'N/A')}")


def main():
    print("Starting Wikimedia stream producer...")
    create_topic("wikimedia.recentchange")

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 2147483647,
        "retry.backoff.ms": 500,
        "delivery.timeout.ms": 60000,
        "compression.type": "lz4",
        "linger.ms": 5,
        "batch.size": 32768,
        "max.in.flight.requests.per.connection": 5,
        # Buffer Settings Fixed:
        "queue.buffering.max.kbytes": 65536  # 64 MB buffer limit (65536 KB)
       # "max.block.ms": 30000,                # Wait up to 30s when buffer is full
    }

    producer = Producer(producer_config)

    try:
        stream_wikimedia(producer)
    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        print("Flushing remaining messages...")
        producer.flush()
        print("Producer flushed and exiting.")


if __name__ == "__main__":
    main()