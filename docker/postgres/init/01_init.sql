DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_supplier;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_seller;
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    email VARCHAR(255),
    country VARCHAR(100),
    postal_code VARCHAR(40),
    pet_type VARCHAR(80),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(120)
);

CREATE TABLE dim_seller (
    seller_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    country VARCHAR(100),
    postal_code VARCHAR(40)
);

CREATE TABLE dim_product (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(150),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    quantity INTEGER,
    pet_category VARCHAR(100),
    weight DECIMAL(10, 2),
    color VARCHAR(80),
    size VARCHAR(80),
    brand VARCHAR(100),
    material VARCHAR(100),
    description TEXT,
    rating DECIMAL(4, 2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE dim_store (
    store_id INTEGER PRIMARY KEY,
    store_name VARCHAR(150),
    location VARCHAR(150),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(80),
    email VARCHAR(255)
);

CREATE TABLE dim_supplier (
    supplier_id INTEGER PRIMARY KEY,
    supplier_name VARCHAR(150),
    contact VARCHAR(150),
    email VARCHAR(255),
    phone VARCHAR(80),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE fact_sales (
    sale_id INTEGER PRIMARY KEY,
    source_file VARCHAR(40) NOT NULL,
    source_filename TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    source_row_id INTEGER NOT NULL,
    sale_date DATE,
    customer_id INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    seller_id INTEGER NOT NULL REFERENCES dim_seller(seller_id),
    product_id INTEGER NOT NULL REFERENCES dim_product(product_id),
    store_id INTEGER NOT NULL REFERENCES dim_store(store_id),
    supplier_id INTEGER NOT NULL REFERENCES dim_supplier(supplier_id),
    quantity INTEGER,
    total_price DECIMAL(10, 2),
    product_unit_price DECIMAL(10, 2),
    product_stock_quantity INTEGER,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_sales_sale_date ON fact_sales (sale_date);
CREATE INDEX idx_fact_sales_customer ON fact_sales (customer_id);
CREATE INDEX idx_fact_sales_product ON fact_sales (product_id);
CREATE INDEX idx_fact_sales_store ON fact_sales (store_id);
CREATE INDEX idx_fact_sales_supplier ON fact_sales (supplier_id);
