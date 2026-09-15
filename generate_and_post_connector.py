"""
Registers the Debezium Postgres connector with Kafka Connect via its REST API.
Run this after Kafka Connect is up (docker-compose handles startup ordering).
"""
import json
import time
import requests

KAFKA_CONNECT_URL = "http://kafka-connect:8083/connectors"

CONNECTOR_CONFIG = {
    "name": "banking-postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "banking_user",
        "database.password": "banking_pass",
        "database.dbname": "banking",
        "topic.prefix": "banking",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "publication.autocreate.mode": "filtered",
        "snapshot.mode": "initial",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
    },
}


def wait_for_connect(url, retries=30, delay=5):
    for _ in range(retries):
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(delay)
    raise RuntimeError("Kafka Connect did not become available in time")


def register_connector():
    wait_for_connect(KAFKA_CONNECT_URL)
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(CONNECTOR_CONFIG),
    )
    if resp.status_code in (200, 201):
        print("Connector registered successfully")
    elif resp.status_code == 409:
        print("Connector already exists")
    else:
        print(f"Failed to register connector: {resp.status_code} {resp.text}")
        resp.raise_for_status()


if __name__ == "__main__":
    register_connector()
