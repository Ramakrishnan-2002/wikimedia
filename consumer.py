import json
import logging

from confluent_kafka import Consumer, KafkaError
from opensearchpy import OpenSearch, helpers


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# ============================================================
# Configuration
# ============================================================

KAFKA_TOPIC = "wikimedia.recentchange"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

KAFKA_GROUP_ID = "wikimedia-opensearch-group"

OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 9200

INDEX_NAME = "wikimedia-recentchange"

BATCH_SIZE = 100


# ============================================================
# OpenSearch Client
# ============================================================

def create_opensearch_client():

    client = OpenSearch(
        hosts=[
            {
                "host": OPENSEARCH_HOST,
                "port": OPENSEARCH_PORT
            }
        ],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        ssl_show_warn=False,
    )

    # Check connection
    if not client.ping():
        raise ConnectionError(
            "Could not connect to OpenSearch at "
            f"{OPENSEARCH_HOST}:{OPENSEARCH_PORT}"
        )

    logging.info("Connected to OpenSearch successfully.")

    # Create index if it doesn't exist
    if not client.indices.exists(index=INDEX_NAME):

        client.indices.create(
            index=INDEX_NAME,
            body={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }
        )

        logging.info(
            f"Created OpenSearch index '{INDEX_NAME}'"
        )

    else:
        logging.info(
            f"OpenSearch index '{INDEX_NAME}' already exists."
        )

    return client


# ============================================================
# Process Batch
# ============================================================

def process_batch(opensearch_client, records_batch):

    if not records_batch:
        return True

    actions = []

    for record in records_batch:

        # Wikimedia event ID
        doc_id = (
            record.get("id")
            or record.get("meta", {}).get("id")
        )

        # Skip records without an ID
        if doc_id is None:
            logging.warning(
                "Skipping record because it has no ID."
            )
            continue

        action = {
            "_op_type": "index",
            "_index": INDEX_NAME,
            "_id": str(doc_id),
            "_source": record
        }

        actions.append(action)

    if not actions:
        logging.warning(
            "No valid documents found in batch."
        )
        return True

    try:

        success, failed = helpers.bulk(
            opensearch_client,
            actions,
            stats_only=True
        )

        logging.info(
            f"OpenSearch bulk operation completed. "
            f"Success: {success}, Failed: {failed}"
        )

        # Only consider the batch successful
        # if ALL documents were indexed.
        if failed == 0:
            return True

        logging.error(
            f"{failed} documents failed to index."
        )

        return False

    except Exception as e:

        logging.error(
            f"OpenSearch bulk indexing failed: {e}"
        )

        return False


# ============================================================
# Kafka Consumer
# ============================================================

def create_kafka_consumer():

    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

        "group.id": KAFKA_GROUP_ID,

        # If this consumer group has no committed offset,
        # start from the beginning of the topic.
        "auto.offset.reset": "earliest",

        # We manually commit after successful OpenSearch indexing.
        "enable.auto.commit": False,

        # Optional: identify this consumer.
        "client.id": "wikimedia-opensearch-consumer"
    }

    consumer = Consumer(consumer_config)

    consumer.subscribe([KAFKA_TOPIC])

    logging.info(
        f"Subscribed to Kafka topic '{KAFKA_TOPIC}'"
    )

    return consumer


# ============================================================
# Main Consumer Loop
# ============================================================

def main():

    logging.info(
        "Starting Wikimedia → Kafka → OpenSearch consumer..."
    )

    # --------------------------------------------------------
    # Connect to OpenSearch
    # --------------------------------------------------------

    opensearch_client = create_opensearch_client()

    # --------------------------------------------------------
    # Create Kafka Consumer
    # --------------------------------------------------------

    consumer = create_kafka_consumer()

    batch_buffer = []

    try:

        while True:

            msg = consumer.poll(timeout=1.0)

            # ------------------------------------------------
            # No Kafka message received
            # ------------------------------------------------

            if msg is None:

                # Flush any remaining records
                if batch_buffer:

                    logging.info(
                        f"Flushing partial batch of "
                        f"{len(batch_buffer)} records..."
                    )

                    success = process_batch(
                        opensearch_client,
                        batch_buffer
                    )

                    if success:

                        consumer.commit(
                            asynchronous=False
                        )

                        batch_buffer.clear()

                        logging.info(
                            "Partial batch indexed and "
                            "Kafka offsets committed."
                        )

                    else:

                        logging.error(
                            "OpenSearch failed. "
                            "Kafka offsets NOT committed."
                        )

                        break

                continue

            # ------------------------------------------------
            # Kafka error
            # ------------------------------------------------

            if msg.error():

                if msg.error().code() == KafkaError._PARTITION_EOF:

                    continue

                logging.error(
                    f"Kafka error: {msg.error()}"
                )

                break

            # ------------------------------------------------
            # Deserialize message
            # ------------------------------------------------

            try:

                data = json.loads(
                    msg.value().decode("utf-8")
                )

                batch_buffer.append(data)

            except json.JSONDecodeError as e:

                logging.warning(
                    f"Invalid JSON at "
                    f"partition={msg.partition()}, "
                    f"offset={msg.offset()}: {e}"
                )

                continue

            # ------------------------------------------------
            # Display progress
            # ------------------------------------------------

            logging.info(
                f"Received Kafka message | "
                f"partition={msg.partition()} | "
                f"offset={msg.offset()} | "
                f"batch={len(batch_buffer)}/{BATCH_SIZE}"
            )

            # ------------------------------------------------
            # Process full batch
            # ------------------------------------------------

            if len(batch_buffer) >= BATCH_SIZE:

                logging.info(
                    f"Processing batch of "
                    f"{len(batch_buffer)} records..."
                )

                success = process_batch(
                    opensearch_client,
                    batch_buffer
                )

                # --------------------------------------------
                # IMPORTANT:
                # Commit ONLY after successful indexing.
                # --------------------------------------------

                if success:

                    consumer.commit(
                        asynchronous=False
                    )

                    batch_buffer.clear()

                    logging.info(
                        "Batch successfully indexed. "
                        "Kafka offsets committed."
                    )

                else:

                    logging.error(
                        "Batch indexing failed. "
                        "Kafka offsets NOT committed."
                    )

                    # Stop so messages can be retried
                    # when consumer starts again.
                    break

    except KeyboardInterrupt:

        logging.info(
            "Consumer stopped by user."
        )

    finally:

        # ----------------------------------------------------
        # Process remaining messages before shutdown
        # ----------------------------------------------------

        if batch_buffer:

            logging.info(
                f"Processing final batch of "
                f"{len(batch_buffer)} records..."
            )

            success = process_batch(
                opensearch_client,
                batch_buffer
            )

            if success:

                consumer.commit(
                    asynchronous=False
                )

                logging.info(
                    "Final batch indexed and offsets committed."
                )

            else:

                logging.error(
                    "Final batch failed. "
                    "Offsets were NOT committed."
                )

        # ----------------------------------------------------
        # Close Kafka consumer
        # ----------------------------------------------------

        consumer.close()

        logging.info(
            "Kafka consumer closed."
        )

        logging.info(
            "Consumer shutdown complete."
        )


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()