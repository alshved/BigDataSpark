#!/bin/bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username spark --dbname mockdb <<- 'EOSQL'
CREATE TABLE IF NOT EXISTS mock_data (
    id SERIAL PRIMARY KEY,
    id_csv INTEGER,
    customer_first_name TEXT, customer_last_name TEXT, customer_age INTEGER, customer_email TEXT, customer_country TEXT, customer_postal_code TEXT, customer_pet_type TEXT, customer_pet_name TEXT, customer_pet_breed TEXT,
    seller_first_name TEXT, seller_last_name TEXT, seller_email TEXT, seller_country TEXT, seller_postal_code TEXT,
    product_name TEXT, product_category TEXT, product_price NUMERIC, product_quantity INTEGER,
    sale_date DATE, sale_customer_id INTEGER, sale_seller_id INTEGER, sale_product_id INTEGER, sale_quantity INTEGER, sale_total_price NUMERIC,
    store_name TEXT, store_location TEXT, store_city TEXT, store_state TEXT, store_country TEXT, store_phone TEXT, store_email TEXT,
    pet_category TEXT, product_weight NUMERIC, product_color TEXT, product_size TEXT, product_brand TEXT, product_material TEXT, product_description TEXT, product_rating NUMERIC, product_reviews INTEGER, product_release_date DATE, product_expiry_date DATE,
    supplier_name TEXT, supplier_contact TEXT, supplier_email TEXT, supplier_phone TEXT, supplier_address TEXT, supplier_city TEXT, supplier_country TEXT
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER, email TEXT, country TEXT, postal_code TEXT, pet_type TEXT, pet_name TEXT, pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, email TEXT, country TEXT, postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id INTEGER PRIMARY KEY, name TEXT, category TEXT, price NUMERIC(10,2), quantity INTEGER, weight NUMERIC(10,2), color TEXT, size TEXT, brand TEXT, material TEXT, description TEXT, rating NUMERIC(3,1), reviews INTEGER, release_date DATE, expiry_date DATE, pet_category TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id SERIAL PRIMARY KEY, name TEXT, location TEXT, city TEXT, state TEXT, country TEXT, phone TEXT, email TEXT
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id SERIAL PRIMARY KEY, name TEXT, contact TEXT, email TEXT, phone TEXT, address TEXT, city TEXT, country TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY, full_date DATE UNIQUE, day INTEGER, month INTEGER, year INTEGER, quarter INTEGER, month_name TEXT, day_of_week INTEGER
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES dim_customer(customer_id),
    seller_id INTEGER REFERENCES dim_seller(seller_id),
    product_id INTEGER REFERENCES dim_product(product_id),
    store_id INTEGER REFERENCES dim_store(store_id),
    supplier_id INTEGER REFERENCES dim_supplier(supplier_id),
    date_id INTEGER REFERENCES dim_date(date_id),
    quantity INTEGER,
    total_price NUMERIC(10,2),
    unit_price NUMERIC(10,2)
);
EOSQL

DATA_DIR="/docker-entrypoint-initdb.d/data"
if [ -d "$DATA_DIR" ]; then
    COLUMNS="id_csv,customer_first_name,customer_last_name,customer_age,customer_email,customer_country,customer_postal_code,customer_pet_type,customer_pet_name,customer_pet_breed,seller_first_name,seller_last_name,seller_email,seller_country,seller_postal_code,product_name,product_category,product_price,product_quantity,sale_date,sale_customer_id,sale_seller_id,sale_product_id,sale_quantity,sale_total_price,store_name,store_location,store_city,store_state,store_country,store_phone,store_email,pet_category,product_weight,product_color,product_size,product_brand,product_material,product_description,product_rating,product_reviews,product_release_date,product_expiry_date,supplier_name,supplier_contact,supplier_email,supplier_phone,supplier_address,supplier_city,supplier_country"

    for f in "$DATA_DIR"/*.csv; do
        if [ -f "$f" ]; then
            echo "Importing $f..."
            psql -v ON_ERROR_STOP=1 --username spark --dbname mockdb -c \
            "\copy mock_data($COLUMNS) FROM '$f' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"')"
        fi
    done
else
    echo "Warning: No data directory found at $DATA_DIR"
fi

psql -v ON_ERROR_STOP=1 --username spark --dbname mockdb -c "ALTER TABLE mock_data DROP COLUMN IF EXISTS id_csv;"