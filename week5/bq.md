gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=asia-southeast1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://prefect-dtc/code/9_spark_bigquery.py \
    -- \
        --input_green=gs://prefect-dtc/green/* \
        --input_yellow=gs://prefect-dtc/yellow/* \
        --output=trips_data_all.pyspark_report_all