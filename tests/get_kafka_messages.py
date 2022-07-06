from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
import json
import os
from time import sleep

dotenv_path = os.path.join(*['..']*3, '.env')

#print("athletes data sent: ", json.dumps(dict(zip(columns_athletes, data_athletes))))
#print("hosts data sent: ", json.dumps(dict(zip(columns_hosts, data_hosts))))

# producer.send(topic="athletes", value=b"a").get(timeout=10)
#print("Message sent")

consumer = KafkaConsumer("processed", group_id="test", bootstrap_servers='0.0.0.0:9092', auto_offset_reset='latest')
consumer.subscribe(["processed"])

while True:
    print(consumer.poll(timeout_ms=1000))
    print("Message received")
    sleep(5)

#for m in consumer:
#    print(m.value)
#consumer.close()
#print("Message received")