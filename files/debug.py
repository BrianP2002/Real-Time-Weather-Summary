"""
File: debug.py
Partners: Lin Ha, Haoting Tan
"""

from kafka import KafkaConsumer
from report_pb2 import Report

broker = 'localhost:9092'
topic = "temperatures"

consumer = KafkaConsumer(
    bootstrap_servers = [broker]
)

consumer.subscribe([topic])

for message in consumer:
    report = Report()
    report.ParseFromString(message.value)
    print({
        'partition': message.partition,
        'key': message.key.decode(),
        'date': report.date,
        'degrees': report.degrees
    })