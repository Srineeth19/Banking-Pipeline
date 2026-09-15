# Real-Time Banking Data Pipeline

An end-to-end, CDC-based data pipeline that streams changes from a simulated
banking OLTP database into a cloud warehouse using a Bronze → Silver → Gold
(medallion) architecture — built to practice modern data engineering tooling:
Kafka, Debezium, Airflow, Snowflake, dbt, and GitHub Actions CI/CD.

## Why this project

Operational databases aren't built for analytics, and batch-only ETL means
stale dashboards. This project captures live changes from a Postgres banking
system the moment they happen (via CDC), lands them cheaply in object storage,
and transforms them into analytics-ready fact/dimension tables — with full
history preserved through SCD Type-2 snapshots.

## Architecture

```
Faker data generator
        │  (INSERT/UPDATE)
        ▼
   PostgreSQL (OLTP) ──── WAL ────▶ Debezium (Kafka Connect) ──▶ Kafka topics
                                                                       │
                                                                       ▼
                                                        Kafka → MinIO consumer
                                                          (Bronze: raw JSON)
                                                                       │
                                                                       ▼
                                                    Airflow (scheduled load)
                                                                       │
                                                                       ▼
                                                   Snowflake: Bronze → Silver → Gold
                                                        (dbt staging + marts)
                                                                       │
                                                                       ▼
                                                   dbt snapshots (SCD Type-2)
                                                                       │
                                                                       ▼
                                                        GitHub Actions CI/CD
```

## Tech stack

| Layer            | Tool                                   |
|-------------------|-----------------------------------------|
| Source system      | PostgreSQL                             |
| CDC / streaming     | Debezium + Apache Kafka + Kafka Connect|
| Landing zone        | MinIO (S3-compatible object storage)   |
| Orchestration        | Apache Airflow                        |
| Warehouse             | Snowflake                            |
| Transformation          | dbt (staging, marts, snapshots)    |
| Data simulation           | Python + Faker                   |
| CI/CD                       | GitHub Actions                 |
| Containerization               | Docker / docker-compose      |

## Repository structure

```
banking-pipeline/
├── .github/workflows/         # ci.yml (lint + dbt parse), cd.yml (deploy)
├── banking_dbt/                # dbt project
│   ├── models/staging/          # cleaned, deduplicated CDC views
│   ├── models/marts/            # dim_customers, dim_accounts, fct_transactions
│   ├── snapshots/                # SCD Type-2 history for customers & accounts
│   └── dbt_project.yml
├── consumer/
│   └── kafka_to_minio.py        # Kafka → MinIO bronze landing
├── data-generator/
│   ├── config.yaml               # volume knobs (customers/accounts/txns)
│   └── faker_generator.py       # synthetic banking data + ongoing live updates
├── docker/dags/
│   ├── minio_to_snowflake_dag.py # Bronze load + triggers dbt run/test
│   └── scd_snapshots_dag.py      # hourly dbt snapshot run
├── kafka-debezium/
│   └── generate_and_post_connector.py  # registers the Debezium connector
├── postgres/
│   └── schema.sql                # OLTP schema (customers, accounts, transactions)
├── docker-compose.yml
├── dockerfile-airflow.dockerfile
├── requirements.txt
└── README.md
```

## How it works

1. **Data generation** — `faker_generator.py` seeds ~2,000 customers, a few
   accounts each, and ~100,000 transactions into Postgres, then keeps
   emitting live balance updates so there's a continuous CDC stream.
2. **CDC capture** — Debezium tails the Postgres WAL and publishes row-level
   INSERT/UPDATE events to Kafka topics, one per table.
3. **Bronze landing** — a lightweight Kafka consumer batches events and
   writes them as newline-delimited JSON into MinIO, partitioned by table
   and date.
4. **Orchestration** — Airflow loads each day's Bronze partition into
   Snowflake raw tables in parallel, then triggers dbt.
5. **Transformation** — dbt staging models deduplicate and type the raw CDC
   payloads; mart models build a star schema (`dim_customers`,
   `dim_accounts`, an incrementally-merged `fct_transactions`).
6. **History tracking** — dbt snapshots run on their own hourly DAG to
   maintain SCD Type-2 history for customers and accounts.
7. **CI/CD** — GitHub Actions lints the Python, validates the dbt project on
   every PR, and deploys/tests/snapshots on merge to `main`.

## Running locally

```bash
git clone <this-repo>
cd banking-pipeline
docker-compose up -d postgres zookeeper kafka kafka-connect minio
docker-compose up connector-registrar   # registers the Debezium connector
docker-compose up data-generator         # seeds + streams synthetic data
docker-compose up kafka-to-minio-consumer
docker-compose up airflow-webserver      # http://localhost:8080
```

Snowflake credentials are read from environment variables
(`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`) — set these in
a `.env` file locally and as GitHub Actions secrets for CI/CD, never
committed to the repo.

## Notes

This project was built as a hands-on way to practice the CDC + medallion
architecture pattern that's common in modern data engineering stacks
(source DB → CDC → object storage → warehouse → dbt), inspired by
real-world streaming pipeline designs used in banking/fintech data teams.
