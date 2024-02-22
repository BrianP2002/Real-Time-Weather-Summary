"""
File: consumer.py
Partners: Lin Ha, Haoting Tan
"""

import json
import os
import sys
import time, datetime
from kafka import KafkaConsumer, TopicPartition
from report_pb2 import Report

broker = 'localhost:9092'
topic = "temperatures"

def load_or_initialize_json_file(p):
    path = f"files/partition-{p}.json"
    if not os.path.exists(path):
        data = {
            "partition": p,
            "offset": 0
        }
        with open(path, 'w') as f:
            json.dump(data, f)
    with open(path, 'r') as f:
        return json.load(f)

def update_stat(parition_content, messages):
    for message in messages:
        report = Report()
        report.ParseFromString(message.value)
        year, month, day = report.date.split("-")
        month_key = time.strftime('%B', time.strptime(month, "%m"))
        date = datetime.datetime.strptime(report.date, '%Y-%m-%d')

        if month_key not in parition_content.keys():
            parition_content[month_key] = {}
        if year not in parition_content[month_key]:
            parition_content[month_key][year] = {
                "count": 0,
                "sum": 0.0,
                "avg": 0.0,
                "start": report.date,
                "end": report.date
            }
        end_date = datetime.datetime.strptime(parition_content[month_key][year]["end"], '%Y-%m-%d')
        start_date = datetime.datetime.strptime(parition_content[month_key][year]["start"], '%Y-%m-%d')
        if date <= end_date and parition_content[month_key][year]["count"] != 0:
            continue

        parition_content[month_key][year]["sum"] += report.degrees
        parition_content[month_key][year]["count"] += 1
        parition_content[month_key][year]["avg"] = parition_content[month_key][year]["sum"]/parition_content[month_key][year]["count"]
        if date < start_date:
            parition_content[month_key][year]["start"] = report.date
        if date > end_date:
            parition_content[month_key][year]["end"] = report.date

def save_to_json(json_content):
    partition_num = json_content["partition"]
    path = f"files/partition-{partition_num}.json"
    tmp_path = path + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(json_content, f)
    os.rename(tmp_path, path)

def main():
    p_num = [int(arg) for arg in sys.argv[1:]]
    consumer = KafkaConsumer(bootstrap_servers = [broker])
    consumer.assign([TopicPartition(topic, p) for p in p_num])

    partitions = dict()
    for p in p_num:
        json_content = load_or_initialize_json_file(p)
        partitions[p] = json_content
        offset = json_content["offset"]
        consumer.seek(TopicPartition(topic, p), offset)

    while True:
        batch = consumer.poll(1000)
        for topic_partition, messages in batch.items():
            partition_num = topic_partition.partition
            partition_content = partitions[partition_num]
            update_stat(partition_content, messages)
            partitions[partition_num]["offset"] = consumer.position(topic_partition)
            save_to_json(partitions[partition_num])


if __name__ == "__main__":
    main()