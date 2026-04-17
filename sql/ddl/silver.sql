CREATE TABLE IF NOT EXISTS silver_geolocations (
    zip_code_prefix INT UNSIGNED NOT NULL,
    city VARCHAR(100) NULL,
    state CHAR(2) NULL,
    latitude DECIMAL(10,7) NULL,
    longitude DECIMAL(10,7) NULL,
    PRIMARY KEY (zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS silver_product_categories (
    product_category_name VARCHAR(150) NOT NULL,
    product_category_name_english VARCHAR(150) NULL,
    PRIMARY KEY (product_category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id CHAR(32) NOT NULL,
    customer_unique_id CHAR(32) NOT NULL,
    customer_zip_code_prefix INT UNSIGNED NULL,
    customer_city VARCHAR(100) NULL,
    customer_state CHAR(2) NULL,
    PRIMARY KEY (customer_id),
    KEY idx_silver_customers_unique_id (customer_unique_id),
    KEY idx_silver_customers_zip (customer_zip_code_prefix),
    CONSTRAINT fk_silver_customers_geolocations
        FOREIGN KEY (customer_zip_code_prefix)
        REFERENCES silver_geolocations(zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS silver_sellers (
    seller_id CHAR(32) NOT NULL,
    seller_zip_code_prefix INT UNSIGNED NULL,
    seller_city VARCHAR(100) NULL,
    seller_state CHAR(2) NULL,
    PRIMARY KEY (seller_id),
    KEY idx_silver_sellers_zip (seller_zip_code_prefix),
    CONSTRAINT fk_silver_sellers_geolocations
        FOREIGN KEY (seller_zip_code_prefix)
        REFERENCES silver_geolocations(zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS silver_products (
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
    KEY idx_silver_products_category (product_category_name),
    CONSTRAINT fk_silver_products_categories
        FOREIGN KEY (product_category_name)
        REFERENCES silver_product_categories(product_category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS silver_orders (
    order_id CHAR(32) NOT NULL,
    customer_id CHAR(32) NOT NULL,
    order_status VARCHAR(32) NULL,
    order_purchase_timestamp DATETIME NULL,
    order_approved_at DATETIME NULL,
    order_delivered_carrier_date DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL,
    PRIMARY KEY (order_id),
    KEY idx_silver_orders_customer (customer_id),
    KEY idx_silver_orders_purchase (order_purchase_timestamp),
    CONSTRAINT fk_silver_orders_customers
        FOREIGN KEY (customer_id)
        REFERENCES silver_customers(customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS silver_order_items (
    order_id CHAR(32) NOT NULL,
    order_item_id INT UNSIGNED NOT NULL,
    product_id CHAR(32) NOT NULL,
    seller_id CHAR(32) NOT NULL,
    shipping_limit_date DATETIME NULL,
    price DECIMAL(14,2) NOT NULL,
    freight_value DECIMAL(14,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id),
    KEY idx_silver_order_items_product (product_id),
    KEY idx_silver_order_items_seller (seller_id),
    CONSTRAINT fk_silver_order_items_orders
        FOREIGN KEY (order_id)
        REFERENCES silver_orders(order_id),
    CONSTRAINT fk_silver_order_items_products
        FOREIGN KEY (product_id)
        REFERENCES silver_products(product_id),
    CONSTRAINT fk_silver_order_items_sellers
        FOREIGN KEY (seller_id)
        REFERENCES silver_sellers(seller_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS silver_order_payments (
    order_id CHAR(32) NOT NULL,
    payment_sequential INT UNSIGNED NOT NULL,
    payment_type VARCHAR(32) NULL,
    payment_installments INT UNSIGNED NULL,
    payment_value DECIMAL(14,2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential),
    CONSTRAINT fk_silver_order_payments_orders
        FOREIGN KEY (order_id)
        REFERENCES silver_orders(order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS silver_order_reviews (
    review_id CHAR(32) NOT NULL,
    order_id CHAR(32) NOT NULL,
    review_score TINYINT UNSIGNED NULL,
    review_comment_title VARCHAR(255) NULL,
    review_comment_message TEXT NULL,
    review_creation_date DATETIME NULL,
    review_answer_timestamp DATETIME NULL,
    PRIMARY KEY (review_id, order_id),
    KEY idx_silver_order_reviews_order (order_id),
    CONSTRAINT fk_silver_order_reviews_orders
        FOREIGN KEY (order_id)
        REFERENCES silver_orders(order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
