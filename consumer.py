from opensearchpy import OpenSearch
import logging
from confluent_kafka import Consumer, KafkaError
import json


logging.basicConfig(
    level=logging.INFO,  # INFO level shows normal operations; use DEBUG for more detail
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()  # logs will appear in your console/terminal
    ]
)

logger=logging.getLogger(__name__)

def create_os_index(index_name :str, client: OpenSearch):
    
    if not client.ping():
        raise ConnectionError("Could not connect to Host : localhost, Port: 9200")
    logger.info("Opensearch connected successfully")

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
                "timestamp": {"type": "date"}
            }
        }
    }

    if client.indices.exists(index=index_name):
        logger.info(f"{index_name} Index already exists")
    else:
        client.indices.create(index=index_name,body=index_body)
        logger.info(f"A new {index_name} Index has been created successfully")

def insert_to_os(client : OpenSearch, index_name : str, message):
    try:
        doc= json.loads(message.decode("utf-8"))
        doc_id = str(doc.get("id",None))
        client.index(index=index_name,id=doc_id,body=doc)
        logger.info(f"Indexed document with id={doc_id}")
    except Exception as e:
        logger.error(f"Failed to index document: {e}")

def create_kafka_consumer(topic_name,client:OpenSearch):
    conf ={
        'bootstrap.servers':'localhost:9092',
        'group.id': 'consumer-demo-grp',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer=Consumer(conf)
    consumer.subscribe([topic_name])
    try:
        while True:
            message=consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaError(message.error())
            else:
                #logger.info(f"Received message, \n Key:{message.key()}, Value:{message.value()},Topic:{message.topic()},Partition:{message.partition()},Offset:{message.offset()} ")
                insert_to_os(client,'wikimedia.recentchanges',message.value())
                consumer.commit(message)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    client = OpenSearch(
            hosts=[{"host":"localhost","port":9200}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False
        )
    create_os_index('wikimedia.recentchanges',client)
    create_kafka_consumer('wikimedia.recentchange',client)

if __name__=='__main__':
    main()