-- Banking OLTP schema (source system for CDC)
-- Enables logical replication so Debezium can stream WAL changes.

CREATE TABLE IF NOT EXISTS customers (
    customer_id     SERIAL PRIMARY KEY,
    first_name      VARCHAR(50)  NOT NULL,
    last_name       VARCHAR(50)  NOT NULL,
    email           VARCHAR(120) UNIQUE NOT NULL,
    phone           VARCHAR(20),
    date_of_birth   DATE,
    address         VARCHAR(200),
    city            VARCHAR(80),
    state           VARCHAR(80),
    country         VARCHAR(80),
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id      SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(customer_id),
    account_type    VARCHAR(20) NOT NULL CHECK (account_type IN ('CHECKING','SAVINGS','CREDIT')),
    account_status  VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (account_status IN ('ACTIVE','CLOSED','FROZEN')),
    balance         NUMERIC(14,2) NOT NULL DEFAULT 0,
    currency        VARCHAR(3)  NOT NULL DEFAULT 'USD',
    opened_at       TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   BIGSERIAL PRIMARY KEY,
    account_id       INTEGER NOT NULL REFERENCES accounts(account_id),
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('DEPOSIT','WITHDRAWAL','TRANSFER','PAYMENT')),
    amount           NUMERIC(14,2) NOT NULL,
    merchant         VARCHAR(120),
    status           VARCHAR(20) NOT NULL DEFAULT 'COMPLETED' CHECK (status IN ('COMPLETED','PENDING','FAILED')),
    transaction_ts   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_accounts_customer_id ON accounts(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_ts ON transactions(transaction_ts);

-- Debezium requires REPLICA IDENTITY FULL to capture full row on UPDATE/DELETE
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
