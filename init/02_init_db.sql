CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id     INTEGER PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    age             INTEGER,
    email           TEXT,
    country         TEXT,
    postal_code     TEXT,
    pet_type        TEXT,
    pet_name        TEXT,
    pet_breed       TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id       INTEGER PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    email           TEXT,
    country         TEXT,
    postal_code     TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id      INTEGER PRIMARY KEY,
    name            TEXT,
    category        TEXT,
    price           NUMERIC(10,2),
    quantity        INTEGER,
    weight          NUMERIC(10,2),
    color           TEXT,
    size            TEXT,
    brand           TEXT,
    material        TEXT,
    description     TEXT,
    rating          NUMERIC(3,1),
    reviews         INTEGER,
    release_date    DATE,
    expiry_date     DATE,
    pet_category    TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id        SERIAL PRIMARY KEY,
    name            TEXT,
    location        TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    phone           TEXT,
    email           TEXT
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id     SERIAL PRIMARY KEY,
    name            TEXT,
    contact         TEXT,
    email           TEXT,
    phone           TEXT,
    address         TEXT,
    city            TEXT,
    country         TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    full_date       DATE UNIQUE,
    day             INTEGER,
    month           INTEGER,
    year            INTEGER,
    quarter         INTEGER,
    month_name      TEXT,
    day_of_week     INTEGER
);


CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id         SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES dim_customer(customer_id),
    seller_id       INTEGER REFERENCES dim_seller(seller_id),
    product_id      INTEGER REFERENCES dim_product(product_id),
    store_id        INTEGER REFERENCES dim_store(store_id),
    supplier_id     INTEGER REFERENCES dim_supplier(supplier_id),
    date_id         INTEGER REFERENCES dim_date(date_id),
    quantity        INTEGER,
    total_price     NUMERIC(10,2),
    unit_price      NUMERIC(10,2)
);