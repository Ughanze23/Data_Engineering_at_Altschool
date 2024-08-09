-- create schema for each entity
CREATE SCHEMA IF NOT EXISTS  orders;
CREATE SCHEMA IF NOT EXISTS  customers;
CREATE SCHEMA IF NOT EXISTS  products;
CREATE SCHEMA IF NOT EXISTS  sellers;
CREATE SCHEMA IF NOT EXISTS  geo_location;
CREATE SCHEMA IF NOT EXISTS  staging;

--create orders table
CREATE TABLE IF NOT EXISTS orders.orders(
    order_id VARCHAR(255) NOT NULL PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    order_status VARCHAR(255) NOT NULL,
    order_purchase_timestamp TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    order_approved_at TIMESTAMP(0) WITHOUT TIME ZONE NULL,
    order_delivered_carrier_date TIMESTAMP(0) WITHOUT TIME ZONE NULL,
    order_delivered_customer_date TIMESTAMP(0) WITHOUT TIME ZONE NULL,
    order_estimated_delivery_date TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    UNIQUE (customer_id)
);

-- create order items table
CREATE TABLE IF NOT EXISTS orders.order_items(
    order_id VARCHAR(255) NOT NULL ,
    order_item_id INTEGER NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    seller_id VARCHAR(255) NOT NULL,
    shipping_limit_date TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    price FLOAT(53) NOT NULL,
    freight_value FLOAT(53) NOT NULL
);

--create order reviews table
CREATE TABLE IF NOT EXISTS orders.order_reviews(
    review_id VARCHAR(255) NOT NULL ,
    order_id VARCHAR(255) NOT NULL,
    review_score INTEGER NOT NULL,
    review_comment_title TEXT NULL,
    review_comment_message TEXT NULL,
    review_creation_date TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    review_answer_timestamp TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.products(
 product_id VARCHAR(255) NOT NULL PRIMARY KEY,
    product_category_name VARCHAR(255) NULL,
    product_name_lenght INTEGER NULL,
    product_description_lenght INTEGER NULL,
    product_photos_qty INTEGER NULL,
    product_weight_g FLOAT(53) NULL,
    product_length_cm FLOAT(53) NULL,
    product_height_cm FLOAT(53) NULL,
    product_width_cm FLOAT(53) NULL  
);

--create order payments table
CREATE TABLE IF NOT EXISTS orders.order_payments(
    order_id VARCHAR(255) NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type VARCHAR(255) NOT NULL,
    payment_installments INTEGER NOT NULL,
    payment_value FLOAT(53) NOT NULL
);

--create customers table
CREATE TABLE IF NOT EXISTS customers.customers(
    customer_id VARCHAR(255) NOT NULL,
    customer_unique_id VARCHAR(255) NOT NULL,
    customer_zip_code_prefix BIGINT NOT NULL,
    customer_city VARCHAR(255) NOT NULL,
    customer_state VARCHAR(255) NOT NULL,
    PRIMARY KEY(customer_id, customer_unique_id)

);

--create products table
CREATE TABLE IF NOT EXISTS products.products(
    product_id VARCHAR(255) NOT NULL PRIMARY KEY,
    category_id VARCHAR(255) NULL,
    product_name_lenght INTEGER NULL,
    product_description_lenght INTEGER NULL,
    product_photos_qty INTEGER NULL,
    product_weight_g FLOAT(53) NULL,
    product_length_cm FLOAT(53) NULL,
    product_height_cm FLOAT(53) NULL,
    product_width_cm FLOAT(53) NULL
);

-- create products category table
CREATE TABLE IF NOT EXISTS products.product_category(
    category_id UUID NOT NULL DEFAULT gen_random_uuid(),
    product_category_name VARCHAR(255) NOT NULL,
    product_category_name_english VARCHAR(255) NOT NULL
);


--create geolocation table.
CREATE TABLE IF NOT EXISTS geo_location.geolocation(
    geolocation_zip_code_prefix BIGINT NOT NULL,
    geolocation_lat FLOAT(53) NOT NULL,
    geolocation_lng FLOAT(53) NOT NULL,
    geolocation_city VARCHAR(255) NOT NULL,
    geolocation_state VARCHAR(255) NOT NULL
);

--create sellers table
CREATE TABLE IF NOT EXISTS sellers.sellers(
    seller_id VARCHAR(255) NOT NULL PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(255) NOT NULL,
    seller_city VARCHAR(255) NOT NULL,
    seller_state VARCHAR(255) NOT NULL
);



--insert data into tables
COPY customers.customers (customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state)
FROM '/data/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;

COPY geo_location.geolocation (geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state)
FROM '/data/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;

COPY orders.order_items (order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value)
FROM '/data/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;

COPY orders.order_payments (order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value)
FROM '/data/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

COPY orders.order_reviews (review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp)
FROM '/data/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';



COPY orders.orders (order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,order_estimated_delivery_date)
FROM '/data/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;

COPY staging.products (product_id, product_category_name,product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm)
FROM '/data/olist_products_dataset.csv' DELIMITER ',' CSV HEADER;

COPY sellers.sellers (seller_id, seller_zip_code_prefix, seller_city,seller_state)
FROM '/data/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;


COPY products.product_category (product_category_name,product_category_name_english)
FROM '/data/product_category_name_translation.csv' DELIMITER ',' CSV HEADER;

INSERT INTO products.products (product_id, category_id,product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm)
SELECT  product_id, cat.category_id,product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm

FROM staging.products stp
LEFT JOIN products.product_category cat ON cat.product_category_name = stp.product_category_name;

DROP TABLE staging.products;