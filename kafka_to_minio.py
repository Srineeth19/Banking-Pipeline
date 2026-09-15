"""
Consumes Debezium CDC events from Kafka topics and lands them as newline-delimited
JSON objects in MinIO (S3-compatible storage), partitioned by table and date.
This is the "Bronze" landing zone that Airflow later loads into Snowflake.
"""
import io
import json
import os
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS = ["banking.public.customers", "banking.public.accounts", "banking.public.transactions"]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "banking-bronze")

FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "500"))

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)


def ensure_bucket():
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)


def table_name_from_topic(topic: str) -> str:
    return topic.split(".")[-1]


def flush_buffer(table: str, records: list):
    if not records:
        return
    now = datetime.now(timezone.utc)
    key = (
        f"{table}/dt={now:%Y-%m-%d}/"
        f"{table}_{now:%H%M%S}_{now.microsecond}.jsonl"
    )
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    s3.upload_fileobj(io.BytesIO(body), BUCKET, key)
    print(f"Flushed {len(records)} records to s3://{BUCKET}/{key}")


def main():
    ensure_bucket()
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="banking-minio-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    buffers = {table_name_from_topic(t): [] for t in TOPICS}

    for message in consumer:
        if message.value is None:
            continue  # tombstone / delete marker, skip in bronze
        table = table_name_from_topic(message.topic)
        payload = message.value.get("payload", message.value)
        buffers[table].append({
            "op": payload.get("op"),
            "after": payload.get("after"),
            "before": payload.get("before"),
            "source_ts_ms": payload.get("ts_ms"),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        })
        if len(buffers[table]) >= FLUSH_EVERY:
            flush_buffer(table, buffers[table])
            buffers[table] = []


if __name__ == "__main__":
    main()
