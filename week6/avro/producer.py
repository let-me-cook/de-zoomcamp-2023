from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from pathlib import Path
from time import sleep


def load_avro_schema_from_file(key_filename, val_filename):
    key_schema = avro.load(Path(".") / "schema" / key_filename)
    val_schema = avro.load(Path(".") / "schema" / val_filename)

    return key_schema, val_schema


if __name__ == "__main__":
    key_filename = "taxi_ride_key.avsc"
    val_filename = "taxi_ride_val.avsc"
    topic = "datatalkclub.yellow_taxi_rides"

    key_sch, val_sch = load_avro_schema_from_file(key_filename, val_filename)

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1",
    }

    producer = AvroProducer(
        producer_config, default_key_schema=key_sch, default_value_schema=val_sch
    )

    file = open(Path("..") / "resources" / "rides.csv")

    csv_reader = csv.reader(file)
    header = next(csv_reader)

    for row in csv_reader:
        key = {"vendor_id": int(row[0])}
        value = {
            "vendor_id": int(row[0]),
            "passenger_count": int(row[3]),
            "trip_distance": float(row[3]),
            "payment_type": int(row[9]),
            "total_amount": float(row[16]),
        }

        try:
            producer.produce(
                topic=topic,
                key=key,
                value=value
            )
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")
        
        producer.flush()
        sleep(1)
    
