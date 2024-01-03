import pandas as pd
from confluent_kafka import Producer
import json

# Kafka producer configuration
kafka_producer_config = {
    'bootstrap.servers': 'pkc-4r087.us-west2.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username' : 'IC36SACAFDAJF3XK',
    'sasl.password' : 'V3SFQOJTbhkplNV82nM1akV2RZSnhdfyxpyhY1sOvTX++iMWzvF/zx8xa1utwudd',
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 1000,
    'linger.ms': 5,
    'batch.num.messages': 1000
}

# Create Kafka producer
producer = Producer(kafka_producer_config)

# Kafka topic
kafka_topic = 'health_data'

# Read the dataset from a CSV file
dataset_path = 'mock_heart_health_dataset.csv'
df = pd.read_csv(dataset_path)

# Optional per-message delivery callback
def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
        if isinstance(err, KafkaException) and err.code() == KafkaException._PARTITION_EOF:
            # If a partition EOF is encountered, you can handle it here
            pass
    else:
        print("Produced event to topic {topic}: key = {key} value = {value}".format(
            topic=msg.topic(), key=msg.key(), value=msg.value()))

# Assuming you have loaded your dataset into 'df'
downsampled_df = df.sample(frac=0.1)  # Adjust the fraction as needed

# Produce data from the dataset to Kafka topic
for _, row in downsampled_df.iterrows():
    data = row.to_dict()
    key = str(row['User ID'])  # Assuming 'User ID' is a suitable key
    value = json.dumps(data).encode('utf-8')
    
    while True:
        try:
            producer.produce(kafka_topic, key=key, value=value, callback=delivery_callback)
            break
        except BufferError as e:
            print('Local queue is full. Waiting for space...')
            time.sleep(100000000)

# Flush to ensure all messages are sent
producer.flush()

