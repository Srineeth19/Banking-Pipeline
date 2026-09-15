"""
Simulates a banking OLTP workload: customers, accounts, and transactions.
Writes into Postgres in batches so Debezium/Kafka has a continuous stream
of INSERT/UPDATE change events to capture.
"""
import random
import time
import yaml
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

fake = Faker()


def load_config(path="config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def get_connection(cfg):
    pg = cfg["postgres"]
    return psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["dbname"],
        user=pg["user"], password=pg["password"],
    )


def generate_customers(conn, n, batch_size):
    rows = []
    for _ in range(n):
        rows.append((
            fake.first_name(), fake.last_name(), fake.unique.email(),
            fake.phone_number()[:20], fake.date_of_birth(minimum_age=18, maximum_age=85),
            fake.street_address(), fake.city(), fake.state(), fake.country(),
        ))
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            execute_values(cur, """
                INSERT INTO customers
                    (first_name, last_name, email, phone, date_of_birth,
                     address, city, state, country)
                VALUES %s
            """, rows[i:i + batch_size])
    conn.commit()
    print(f"Inserted {n} customers")


def generate_accounts(conn, cfg):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM customers")
        customer_ids = [r[0] for r in cur.fetchall()]

    rows = []
    for cid in customer_ids:
        for _ in range(random.randint(1, cfg["num_accounts_per_customer_max"])):
            rows.append((
                cid, random.choice(cfg["account_types"]), "ACTIVE",
                round(random.uniform(0, 50000), 2), "USD",
            ))
    with conn.cursor() as cur:
        for i in range(0, len(rows), cfg["batch_size"]):
            execute_values(cur, """
                INSERT INTO accounts (customer_id, account_type, account_status, balance, currency)
                VALUES %s
            """, rows[i:i + cfg["batch_size"]])
    conn.commit()
    print(f"Inserted {len(rows)} accounts")


def generate_transactions(conn, cfg):
    with conn.cursor() as cur:
        cur.execute("SELECT account_id FROM accounts")
        account_ids = [r[0] for r in cur.fetchall()]

    batch = []
    inserted = 0
    with conn.cursor() as cur:
        for _ in range(cfg["num_transactions"]):
            batch.append((
                random.choice(account_ids),
                random.choice(cfg["transaction_types"]),
                round(random.uniform(-2000, 5000), 2),
                fake.company(),
                random.choices(["COMPLETED", "PENDING", "FAILED"], weights=[90, 7, 3])[0],
                fake.date_time_between(start_date="-90d", end_date="now"),
            ))
            if len(batch) >= cfg["batch_size"]:
                execute_values(cur, """
                    INSERT INTO transactions
                        (account_id, transaction_type, amount, merchant, status, transaction_ts)
                    VALUES %s
                """, batch)
                conn.commit()
                inserted += len(batch)
                print(f"Inserted {inserted}/{cfg['num_transactions']} transactions")
                batch = []
        if batch:
            execute_values(cur, """
                INSERT INTO transactions
                    (account_id, transaction_type, amount, merchant, status, transaction_ts)
                VALUES %s
            """, batch)
            conn.commit()
            inserted += len(batch)
    print(f"Inserted {inserted} transactions total")


def simulate_live_updates(conn, cfg, iterations=200, delay_seconds=2):
    """Emits ongoing UPDATEs/INSERTs after the initial bulk load, so Debezium
    keeps streaming CDC events instead of the pipeline going idle."""
    with conn.cursor() as cur:
        cur.execute("SELECT account_id FROM accounts")
        account_ids = [r[0] for r in cur.fetchall()]

    for i in range(iterations):
        with conn.cursor() as cur:
            acc = random.choice(account_ids)
            delta = round(random.uniform(-500, 500), 2)
            cur.execute(
                "UPDATE accounts SET balance = balance + %s, updated_at = now() WHERE account_id = %s",
                (delta, acc),
            )
            cur.execute("""
                INSERT INTO transactions (account_id, transaction_type, amount, merchant, status, transaction_ts)
                VALUES (%s, %s, %s, %s, %s, now())
            """, (acc, random.choice(cfg["transaction_types"]), delta, fake.company(), "COMPLETED"))
        conn.commit()
        if i % 20 == 0:
            print(f"Live update batch {i}/{iterations}")
        time.sleep(delay_seconds)


if __name__ == "__main__":
    cfg = load_config()
    conn = get_connection(cfg)
    generate_customers(conn, cfg["num_customers"], cfg["batch_size"])
    generate_accounts(conn, cfg)
    generate_transactions(conn, cfg)
    simulate_live_updates(conn, cfg)
    conn.close()
