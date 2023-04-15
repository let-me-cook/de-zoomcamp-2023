from confluent_kafka.avro import AvroConsumer

if __name__ == "__main__":
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "group.id": "datatalkclubs.taxirides.avro.consumer.2",
        "auto.offset.reset": "earliest",
    }

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["datatalkclub.yellow_taxi_rides"])

    while True:
        try:
            message = consumer.poll(timeout=5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                print(
                    f"Successfully poll a record from Kafka topic: {message.topic()},"
                    f" partition: {message.partition()}, offset:"
                    f" {message.offset()}\nmessage key: {message.key()} || message"
                    f" value: {message.value()}"
                )
                consumer.commit()
            else:
                print("No new messages at this point. Try again later.")
