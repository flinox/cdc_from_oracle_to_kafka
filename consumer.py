# consumer kafka in python3
from kafka import KafkaConsumer
from json import loads

collection = []

consumer = KafkaConsumer(
    'OLIMPO-ALUNCURS',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='CONTROLE-OLIMPO-ALUNCURS',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     consumer_timeout_ms=30000)

for message in consumer:
    message = message.value
    collection.append(message)
    #consumer.commit()
    print('{}'.format(message))     