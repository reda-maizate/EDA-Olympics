from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
import json
import os
from time import sleep

dotenv_path = os.path.join(*['..']*3, '.env')

#admin_client = KafkaAdminClient(bootstrap_servers=f'0.0.0.0:{KAFKA_PRODUCER_PORT}', api_version=(0, 10, 2))

producer = KafkaProducer(bootstrap_servers='0.0.0.0:9092', api_version=(0, 10, 2))

columns_athletes = ["athlete_url", "athlete_full_name", "first_game", "athlete_year_birth", "athlete_medals", "games_participations"]
data_athletes = ["https://olympics.com/en/athletes/dongqi-chen","Dongqi CHEN","Tokyo 2020","1988.0","2B", "3"]
columns_hosts = ["game_slug", "game_end_date", "game_start_date", "game_location", "game_name", "game_season", "game_year"]
data_hosts = ["tokyo-2020","2021-08-08T14:00:00Z","2021-07-23T11:00:00Z","Japan", "Tokyo 2020", "Summer", "2020"]

print("athletes data sent: ", json.dumps(dict(zip(columns_athletes, data_athletes))))
print("hosts data sent: ", json.dumps(dict(zip(columns_hosts, data_hosts))))

while True:
    producer.send(topic="athletes", value=json.dumps(dict(zip(columns_athletes, data_athletes))).encode('utf-8'))
    producer.send(topic="hosts", value=json.dumps(dict(zip(columns_hosts, data_hosts))).encode('utf-8'))
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