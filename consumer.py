from opensearchpy import OpenSearch
import logging


logging.basicConfig(
    level=logging.INFO,  # INFO level shows normal operations; use DEBUG for more detail
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()  # logs will appear in your console/terminal
    ]
)

logger=logging.getLogger(__name__)

def create_os_index(index_name :str, ):
    client = OpenSearch(
        hosts=[{"host":"localhost","port":9200}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False
    )
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



def main():
    create_os_index('wikimedia.recentchanges')

if __name__=='__main__':
    main()