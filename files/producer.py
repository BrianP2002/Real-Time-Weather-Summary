"""
File: producer.py
Partners: Lin Ha, Haoting Tan
"""

import weather
import time
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from report_pb2 import Report


broker = 'localhost:9092'
topic = "temperatures"

def main():
    admin_client = KafkaAdminClient(bootstrap_servers=[broker])

    try:
        admin_client.delete_topics([topic])
        print("Deleted topics successfully")
    except UnknownTopicOrPartitionError:
        print("Cannot delete topic/s (may not exist yet)")

    time.sleep(3)

    admin_client.create_topics([NewTopic(
        name = topic, 
        num_partitions = 4, 
        replication_factor = 1
    )])

    print("Topics:", admin_client.list_topics())

    producer = KafkaProducer(
        bootstrap_servers = [broker],
        acks = "all",
        retries = 10
    )

    for date, degrees in weather.get_next_weather(delay_sec = 0.1):
        proto_report = Report(date = date, degrees = degrees)
        proto_value = proto_report.SerializeToString()
        month = date.split('-')[1]
        month_key = time.strftime('%B', time.strptime(month, "%m"))
        producer.send(topic, key = month_key.encode(), value = proto_value)

if __name__ == "__main__":
    main()