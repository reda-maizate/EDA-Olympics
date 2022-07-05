from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
import json
import os
from dotenv import load_dotenv

dotenv_path = os.path.join(*['..']*3, '.env')
load_dotenv(dotenv_path)

KAFKA_PRODUCER_PORT = os.getenv('KAFKA_PRODUCER_PORT')

# admin_client = KafkaAdminClient(bootstrap_servers=f'localhost:{KAFKA_PRODUCER_PORT}', api_version=(0, 10, 2))
# admin_client.list_topics()

producer = KafkaProducer(bootstrap_servers='0.0.0.0:9092', api_version=(0, 10, 2))

# columns = ["link", "firstname", "name", "event", "year", "medal1", "medal2", "medal3", "medal4"]
# data = ["https://olympics.com/en/athletes/dongqi-chen","Dongqi" "CHEN","Tokyo 2020",1988.0,0,0,0,0]

# # producer.send(topic="athletes", value=json.dumps(dict(zip(columns, data))))

producer.send(topic="athletes", value=b"a").get(timeout=10)
