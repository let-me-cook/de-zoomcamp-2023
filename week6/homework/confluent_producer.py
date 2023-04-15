from confluent_kafka import Producer

import argparse
import csv
from typing import List, Tuple
from time import sleep

from settings import (
    CONFLUENT_CLOUD_CONFIG,
    GREEN_TAXI_TOPIC,
    FHV_TAXI_TOPIC,
    GREEN_TRIP_DATA_PATH,
    FHV_TRIP_DATA_PATH,
)


def parse_row(ride_type: str, row):
    if ride_type == "green":
        record = f"{row[5]}, {row[6]}"
        key = str(row[0])
    elif ride_type == "fhv":
        record = f"{row[3]}, {row[4]}"
        key = str(row[0])

    return key, record


def read_records(resource_path: str, ride_type: str):
    ride_keys, ride_records = [], []

    with open(resource_path, "r") as f:
        reader = csv.reader(f)
        header = next(reader)

        for row in reader:
            key, record = parse_row(ride_type, row)
            ride_keys.append(key)
            ride_records.append(record)

    return zip(ride_keys, ride_records)


def publish(producer: Producer, records: List[Tuple[str, str]], topic: str):
    for key, value in records:
        try:
            producer.poll(0)
            producer.produce(topic=topic, key=key, value=value)
            print(f"Producing record for <key: {key}, value:{value}>")
        except KeyboardInterrupt:
            break
        except BufferError:
            producer.poll(0.1)
        except Exception as e:
            print(f"Exception while producing record - {value}: {e}")

    producer.flush()
    sleep(10)


def main():
    parser = argparse.ArgumentParser(description="Kafka Consumer")
    parser.add_argument("--type", type=str, default="green")
    args = parser.parse_args()

    if args.type == "green":
        topic = GREEN_TAXI_TOPIC
        datapath = GREEN_TRIP_DATA_PATH
    elif args.type == "fhv":
        topic = FHV_TAXI_TOPIC
        datapath = FHV_TRIP_DATA_PATH

    producer = Producer(CONFLUENT_CLOUD_CONFIG)
    ride_records = read_records(datapath, args.type)
    publish(producer, ride_records, topic)

if __name__ == "__main__":
    main()