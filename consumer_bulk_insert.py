from confluent_kafka import Consumer, KafkaError
import json, logging, uuid
from opensearchpy import OpenSearch,helpers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def create_os_client(index_name:str, client:OpenSearch):
    if not client.ping():
        raise ConnectionError("OpenSearch is not connecting...\n")
    logger.info("Opensearch has been connected succcessfully")
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "type": {"type": "keyword"},
                "title": {"type": "text"},
                "timestamp": {"type": "date"},
                "log_params": {"type": "text"} 
            }
        }
    }
    if client.indices.exists(index=index_name):
        logger.info(f"{index_name} already exists")
    else:
        client.indices.create(index=index_name,body=index_body)
        logger.info(f"{index_name} has been created successfully")

def clean_document(doc: dict) -> dict:
    """
    Clean problematic fields before indexing.
    Convert log_params and other nested objects into strings.
    """
    if "log_params" in doc:
        try:
            doc["log_params"] = json.dumps(doc["log_params"])
        except Exception:
            doc["log_params"] = str(doc["log_params"])
    return doc

def bulk_insert(client, index_name, buffer):
    actions = []
    for doc in buffer:
        doc = clean_document(doc)
        # Try to get id, fallback to a random UUID
        doc_id = str(doc.get("id", str(uuid.uuid4())))
        actions.append({
            "_index": index_name,
            "_id": doc_id,
            "_source": doc
        })
        logger.info(f"Prepared document with id={doc_id}")
    helpers.bulk(client=client,actions=actions)
    logger.info(f"Bulk indexed {len(buffer)} documents into {index_name}")

def create_kafka_consumer(topic_name,index_name,client:OpenSearch,batch_size=100):
    conf ={
        'bootstrap.servers':'localhost:9092',
        'group.id': 'consumer-demo-grp2',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    consumer=Consumer(conf)
    consumer.subscribe([topic_name])
    buffer=[]
    try:
        while True:
            message=consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                logger.error(f"Kafka error: {message.error()}")
            else:
                try:
                    doc=json.loads(message.value().decode('utf-8'))
                    buffer.append(doc)
                    logger.debug(f"Buffered doc id={doc.get('id')}")
                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")
                if len(buffer)>=batch_size:
                    bulk_insert(client,index_name,buffer)
                    consumer.commit()
                    buffer.clear()
    except KeyboardInterrupt:
        pass
    finally:
        if buffer:
            bulk_insert(client, index_name, buffer)
            consumer.commit()
        consumer.close()


def main():
    client = OpenSearch(
            hosts=[{"host":"localhost","port":9200}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False
        )
    create_os_client(index_name="wikimedia.bulk",client=client)
    create_kafka_consumer('wikimedia.recentchange','wikimedia.bulk',client,100)

if __name__=='__main__':
    main()