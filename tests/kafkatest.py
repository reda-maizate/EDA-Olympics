from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
import json
import os
from time import sleep

dotenv_path = os.path.join(*['..']*3, '.env')

#admin_client = KafkaAdminClient(bootstrap_servers=f'0.0.0.0:{KAFKA_PRODUCER_PORT}', api_version=(0, 10, 2))

producer = KafkaProducer(bootstrap_servers='0.0.0.0:9092', api_version=(0, 10, 2))

columns = ["athlete_url", "athlete_full_name", "first_game", "athlete_year_birth", "athlete_medals", "games_participations"]
data = ["https://olympics.com/en/athletes/dongqi-chen","Dongqi CHEN","Tokyo 2020","1988.0","2B", "3"]

print("data sent: ", json.dumps(dict(zip(columns, data))))

while True:
    producer.send(topic="athletes", value=json.dumps(dict(zip(columns, data))).encode('utf-8'))
    print("data sent")
    sleep(5)

# producer.send(topic="athletes", value=b"a").get(timeout=10)
print("Message sent")

# consumer = KafkaConsumer("athletes", group_id="test", bootstrap_servers='0.0.0.0:9092')
# consumer.subscribe(["athletes"])

# print(consumer.poll(timeout_ms=1000))
#for m in consumer:
#    print(m.value)
#consumer.close()
#print("Message received")