CREATE TABLE IF NOT EXISTS gold_dim_customers (
    customer_id CHAR(32) NOT NULL,
    customer_unique_id CHAR(32) NOT NULL,
    customer_zip_code_prefix INT UNSIGNED NULL,
    customer_city VARCHAR(100) NULL,
    customer_state CHAR(2) NULL,
    customer_lat DECIMAL(10,7) NULL,
    customer_lng DECIMAL(10,7) NULL,
    customer_geo_city VARCHAR(100) NULL,
    customer_geo_state CHAR(2) NULL,
    PRIMARY KEY (customer_id),
    KEY idx_gold_dim_customers_unique_id (customer_unique_id),
    KEY idx_gold_dim_customers_zip (customer_zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS gold_dim_products (
    product_id CHAR(32) NOT NULL,
    product_category_name VARCHAR(150) NULL,
    product_name_lenght INT NULL,
    product_description_lenght INT NULL,
    product_photos_qty INT NULL,
    product_weight_g INT NULL,
    product_length_cm INT NULL,
    product_height_cm INT NULL,
    product_width_cm INT NULL,
    product_category_name_english VARCHAR(150) NULL,
    PRIMARY KEY (product_id),
    KEY idx_gold_dim_products_category (product_category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS gold_dim_sellers (
    seller_id CHAR(32) NOT NULL,
    seller_zip_code_prefix INT UNSIGNED NULL,
    seller_city VARCHAR(100) NULL,
    seller_state CHAR(2) NULL,
    seller_lat DECIMAL(10,7) NULL,
    seller_lng DECIMAL(10,7) NULL,
    seller_geo_city VARCHAR(100) NULL,
    seller_geo_state CHAR(2) NULL,
    PRIMARY KEY (seller_id),
    KEY idx_gold_dim_sellers_zip (seller_zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS gold_fct_orders (
    order_id CHAR(32) NOT NULL,
    customer_id CHAR(32) NOT NULL,
    order_status VARCHAR(32) NULL,
    order_purchase_timestamp DATETIME NULL,
    order_approved_at DATETIME NULL,
    order_delivered_carrier_date DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL,
    items_count INT UNSIGNED NULL,
    products_count INT UNSIGNED NULL,
    sellers_count INT UNSIGNED NULL,
    items_value DECIMAL(14,2) NULL,
    freight_value DECIMAL(14,2) NULL,
    order_total_value DECIMAL(14,2) NULL,
    payment_value_total DECIMAL(14,2) NULL,
    payment_sequential_max INT UNSIGNED NULL,
    payment_installments_max INT UNSIGNED NULL,
    payment_installments_mean DECIMAL(12,4) NULL,
    payment_types_nunique INT UNSIGNED NULL,
    payment_type_main VARCHAR(32) NULL,
    review_count INT UNSIGNED NULL,
    review_score_mean DECIMAL(6,4) NULL,
    review_score_min TINYINT UNSIGNED NULL,
    review_score_max TINYINT UNSIGNED NULL,
    has_review_comment BOOLEAN NULL,
    approve_hours DECIMAL(12,4) NULL,
    delivery_days DECIMAL(12,4) NULL,
    carrier_to_customer_days DECIMAL(12,4) NULL,
    estimated_delivery_days DECIMAL(12,4) NULL,
    delivery_delay_days DECIMAL(12,4) NULL,
    is_delivered_late BOOLEAN NULL,
    purchase_year SMALLINT UNSIGNED NULL,
    purchase_month TINYINT UNSIGNED NULL,
    purchase_year_month CHAR(7) NULL,
    PRIMARY KEY (order_id),
    KEY idx_gold_fct_orders_customer (customer_id),
    KEY idx_gold_fct_orders_purchase (order_purchase_timestamp),
    KEY idx_gold_fct_orders_year_month (purchase_year_month),
    CONSTRAINT fk_gold_fct_orders_customers
        FOREIGN KEY (customer_id)
        REFERENCES gold_dim_customers(customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS gold_fct_order_items (
    order_id CHAR(32) NOT NULL,
    order_item_id INT UNSIGNED NOT NULL,
    product_id CHAR(32) NOT NULL,
    seller_id CHAR(32) NOT NULL,
    shipping_limit_date DATETIME NULL,
    price DECIMAL(14,2) NOT NULL,
    freight_value DECIMAL(14,2) NOT NULL,
    item_total_value DECIMAL(14,2) NOT NULL,
    customer_id CHAR(32) NOT NULL,
    order_status VARCHAR(32) NULL,
    order_purchase_timestamp DATETIME NULL,
    order_delivered_carrier_date DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL,
    purchase_year SMALLINT UNSIGNED NULL,
    purchase_month TINYINT UNSIGNED NULL,
    purchase_year_month CHAR(7) NULL,
    delivery_days DECIMAL(12,4) NULL,
    delivery_delay_days DECIMAL(12,4) NULL,
    is_delivered_late BOOLEAN NULL,
    shipping_accuracy_days DECIMAL(12,4) NULL,
    PRIMARY KEY (order_id, order_item_id),
    KEY idx_gold_fct_order_items_product (product_id),
    KEY idx_gold_fct_order_items_seller (seller_id),
    KEY idx_gold_fct_order_items_customer (customer_id),
    KEY idx_gold_fct_order_items_year_month (purchase_year_month),
    CONSTRAINT fk_gold_fct_order_items_customers
        FOREIGN KEY (customer_id)
        REFERENCES gold_dim_customers(customer_id),
    CONSTRAINT fk_gold_fct_order_items_products
        FOREIGN KEY (product_id)
        REFERENCES gold_dim_products(product_id),
    CONSTRAINT fk_gold_fct_order_items_sellers
        FOREIGN KEY (seller_id)
        REFERENCES gold_dim_sellers(seller_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS gold_agg_sales_monthly (
    purchase_year SMALLINT UNSIGNED NOT NULL,
    purchase_month TINYINT UNSIGNED NOT NULL,
    purchase_year_month CHAR(7) NOT NULL,
    orders_count INT UNSIGNED NULL,
    customers_count INT UNSIGNED NULL,
    revenue_total DECIMAL(14,2) NULL,
    payment_total DECIMAL(14,2) NULL,
    freight_total DECIMAL(14,2) NULL,
    avg_ticket DECIMAL(14,2) NULL,
    avg_freight DECIMAL(14,2) NULL,
    avg_delivery_days DECIMAL(12,4) NULL,
    avg_review_score DECIMAL(6,4) NULL,
    delivered_orders INT UNSIGNED NULL,
    late_orders INT UNSIGNED NULL,
    otd_rate DECIMAL(8,6) NULL,
    PRIMARY KEY (purchase_year_month),
    KEY idx_gold_agg_sales_monthly_year (purchase_year),
    KEY idx_gold_agg_sales_monthly_month (purchase_month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
