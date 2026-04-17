CREATE TABLE IF NOT EXISTS bronze_customers (
    customer_id CHAR(32) NOT NULL,
    customer_unique_id CHAR(32) NOT NULL,
    customer_zip_code_prefix INT UNSIGNED NULL,
    customer_city VARCHAR(100) NULL,
    customer_state CHAR(2) NULL,
    PRIMARY KEY (customer_id),
    KEY idx_bronze_customers_unique_id (customer_unique_id),
    KEY idx_bronze_customers_zip (customer_zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bronze_geolocation (
    bronze_geolocation_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    geolocation_zip_code_prefix INT UNSIGNED NULL,
    geolocation_lat DECIMAL(10,7) NULL,
    geolocation_lng DECIMAL(10,7) NULL,
    geolocation_city VARCHAR(100) NULL,
    geolocation_state CHAR(2) NULL,
    PRIMARY KEY (bronze_geolocation_id),
    KEY idx_bronze_geolocation_zip (geolocation_zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bronze_orders (
    order_id CHAR(32) NOT NULL,
    customer_id CHAR(32) NOT NULL,
    order_status VARCHAR(32) NULL,
    order_purchase_timestamp DATETIME NULL,
    order_approved_at DATETIME NULL,
    order_delivered_carrier_date DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL,
    PRIMARY KEY (order_id),
    KEY idx_bronze_orders_customer (customer_id),
    KEY idx_bronze_orders_purchase (order_purchase_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bronze_order_items (
    order_id CHAR(32) NOT NULL,
    order_item_id INT UNSIGNED NOT NULL,
    product_id CHAR(32) NOT NULL,
    seller_id CHAR(32) NOT NULL,
    shipping_limit_date DATETIME NULL,
    price DECIMAL(14,2) NOT NULL,
    freight_value DECIMAL(14,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id),
    KEY idx_bronze_order_items_product (product_id),
    KEY idx_bronze_order_items_seller (seller_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bronze_order_payments (
    order_id CHAR(32) NOT NULL,
    payment_sequential INT UNSIGNED NOT NULL,
    payment_type VARCHAR(32) NULL,
    payment_installments INT UNSIGNED NULL,
    payment_value DECIMAL(14,2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential),
    KEY idx_bronze_order_payments_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bronze_order_reviews (
    review_id CHAR(32) NOT NULL,
    order_id CHAR(32) NOT NULL,
    review_score TINYINT UNSIGNED NULL,
    review_comment_title VARCHAR(255) NULL,
    review_comment_message TEXT NULL,
    review_creation_date DATETIME NULL,
    review_answer_timestamp DATETIME NULL,
    PRIMARY KEY (review_id, order_id),
    KEY idx_bronze_order_reviews_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bronze_products (
    product_id CHAR(32) NOT NULL,
    product_category_name VARCHAR(150) NULL,
    product_name_lenght INT NULL,
    product_description_lenght INT NULL,
    product_photos_qty INT NULL,
    product_weight_g INT NULL,
    product_length_cm INT NULL,
    product_height_cm INT NULL,
    product_width_cm INT NULL,
    PRIMARY KEY (product_id),
    KEY idx_bronze_products_category (product_category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bronze_sellers (
    seller_id CHAR(32) NOT NULL,
    seller_zip_code_prefix INT UNSIGNED NULL,
    seller_city VARCHAR(100) NULL,
    seller_state CHAR(2) NULL,
    PRIMARY KEY (seller_id),
    KEY idx_bronze_sellers_zip (seller_zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bronze_translation (
    product_category_name VARCHAR(150) NOT NULL,
    product_category_name_english VARCHAR(150) NULL,
    PRIMARY KEY (product_category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
