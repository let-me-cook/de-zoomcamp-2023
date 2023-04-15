from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F

from settings import (
    CONFLUENT_CLOUD_CONFIG,
    GREEN_TAXI_TOPIC,
    FHV_TAXI_TOPIC,
    RIDES_TOPIC,
    ALL_RIDE_SCHEMA,
)


def read_from_kafka(spark: SparkSession, topic: str):
    df_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", CONFLUENT_CLOUD_CONFIG["bootstrap.servers"])
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("checkpointLocation", "./checkpoint/")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f"""org.apache.kafka.common.security.plain.PlainLoginModule required username="{CONFLUENT_CLOUD_CONFIG['sasl.username']}" password="{CONFLUENT_CLOUD_CONFIG['sasl.password']}";""",
        )
        .option("failOnDataLoss", False)
        .load()
    )

    return df_stream


def parse_rides(df: DataFrame, schema: StructType):
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    col = F.split(df["value"], ", ")

    for i, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(i).cast(field.dataType))

    df = df.na.drop()
    df.printSchema()

    return df.select([field.name for field in schema])


def sink_console(df: DataFrame, output_mode="complete", processing_time="5 seconds"):
    result = (
        df.writeStream.outputMode(output_mode)
        .trigger(processingTime=processing_time)
        .format("console")
        .option("truncate", False)
        .start()
        .awaitTermination()
    )

    return result


def sink_kafka(df: DataFrame, topic: str, output_mode="complete"):
    query = (
        df
        .writeStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers", CONFLUENT_CLOUD_CONFIG["bootstrap.servers"]
        )
        .outputMode(output_mode)
        .option("topic", topic)
        .option("checkpointLocation", "./checkpoint/")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f"""org.apache.kafka.common.security.plain.PlainLoginModule required username="{CONFLUENT_CLOUD_CONFIG['sasl.username']}" password="{CONFLUENT_CLOUD_CONFIG['sasl.password']}";""",
        )
        .option("failOnDataLoss", False)
        .start()
    )

    return query


def op_groupby(df: DataFrame, column_names: str):
    df_agg = df.groupBy(column_names).count()

    return df_agg


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("streaming-homework")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_green_rides = read_from_kafka(spark, topic=GREEN_TAXI_TOPIC)
    df_fhv_rides = read_from_kafka(spark, topic=FHV_TAXI_TOPIC)

    kafka_sink_green_query = sink_kafka(
        df=df_green_rides, topic=RIDES_TOPIC, output_mode="append"
    )
    kafka_sink_fhv_query = sink_kafka(
        df=df_fhv_rides, topic=RIDES_TOPIC, output_mode="append"
    )

    df_all_rides = read_from_kafka(spark, topic=RIDES_TOPIC)
    df_all_rides = parse_rides(df_all_rides, ALL_RIDE_SCHEMA)

    df_pu_location_count = op_groupby(df_all_rides, ["PULocationID"])
    df_pu_location_count = df_pu_location_count.sort(F.col("count").desc())

    sink_console(df_pu_location_count, output_mode="complete")


if __name__ == "__main__":
    main()
