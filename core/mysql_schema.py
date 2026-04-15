from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection


BRONZE_TABLE_ORDER = [
    "bronze_customers",
    "bronze_geolocation",
    "bronze_orders",
    "bronze_order_items",
    "bronze_order_payments",
    "bronze_order_reviews",
    "bronze_products",
    "bronze_sellers",
    "bronze_translation",
]


SILVER_TABLE_ORDER = [
    "silver_geolocations",
    "silver_product_categories",
    "silver_customers",
    "silver_sellers",
    "silver_products",
    "silver_orders",
    "silver_order_items",
    "silver_order_payments",
    "silver_order_reviews",
]


GOLD_TABLE_ORDER = [
    "gold_dim_customers",
    "gold_dim_products",
    "gold_dim_sellers",
    "gold_fct_orders",
    "gold_fct_order_items",
    "gold_agg_sales_monthly",
]


LAYER_TABLE_ORDER = BRONZE_TABLE_ORDER + SILVER_TABLE_ORDER + GOLD_TABLE_ORDER
LAYER_DELETE_ORDER = list(reversed(LAYER_TABLE_ORDER))


TABLE_COLUMNS = {
    "bronze_customers": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],
    "bronze_geolocation": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ],
    "bronze_orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "bronze_order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ],
    "bronze_order_payments": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ],
    "bronze_order_reviews": [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ],
    "bronze_products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],
    "bronze_sellers": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ],
    "bronze_translation": [
        "product_category_name",
        "product_category_name_english",
    ],
    "silver_geolocations": [
        "zip_code_prefix",
        "city",
        "state",
        "latitude",
        "longitude",
    ],
    "silver_product_categories": [
        "product_category_name",
        "product_category_name_english",
    ],
    "silver_customers": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],
    "silver_sellers": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ],
    "silver_products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],
    "silver_orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "silver_order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ],
    "silver_order_payments": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ],
    "silver_order_reviews": [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ],
    "gold_dim_customers": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "customer_lat",
        "customer_lng",
        "customer_geo_city",
        "customer_geo_state",
    ],
    "gold_dim_products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_category_name_english",
    ],
    "gold_dim_sellers": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "seller_lat",
        "seller_lng",
        "seller_geo_city",
        "seller_geo_state",
    ],
    "gold_fct_orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "items_count",
        "products_count",
        "sellers_count",
        "items_value",
        "freight_value",
        "order_total_value",
        "payment_value_total",
        "payment_sequential_max",
        "payment_installments_max",
        "payment_installments_mean",
        "payment_types_nunique",
        "payment_type_main",
        "review_count",
        "review_score_mean",
        "review_score_min",
        "review_score_max",
        "has_review_comment",
        "approve_hours",
        "delivery_days",
        "carrier_to_customer_days",
        "estimated_delivery_days",
        "delivery_delay_days",
        "is_delivered_late",
        "purchase_year",
        "purchase_month",
        "purchase_year_month",
    ],
    "gold_fct_order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "item_total_value",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "purchase_year",
        "purchase_month",
        "purchase_year_month",
        "delivery_days",
        "delivery_delay_days",
        "is_delivered_late",
        "shipping_accuracy_days",
    ],
    "gold_agg_sales_monthly": [
        "purchase_year",
        "purchase_month",
        "purchase_year_month",
        "orders_count",
        "customers_count",
        "revenue_total",
        "payment_total",
        "freight_total",
        "avg_ticket",
        "avg_freight",
        "avg_delivery_days",
        "avg_review_score",
        "delivered_orders",
        "late_orders",
        "otd_rate",
    ],
}


TABLE_DDL = {
    "bronze_customers": """
        CREATE TABLE IF NOT EXISTS bronze_customers (
            customer_id CHAR(32) NOT NULL,
            customer_unique_id CHAR(32) NOT NULL,
            customer_zip_code_prefix INT UNSIGNED NULL,
            customer_city VARCHAR(100) NULL,
            customer_state CHAR(2) NULL,
            PRIMARY KEY (customer_id),
            KEY idx_bronze_customers_unique_id (customer_unique_id),
            KEY idx_bronze_customers_zip (customer_zip_code_prefix)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "bronze_geolocation": """
        CREATE TABLE IF NOT EXISTS bronze_geolocation (
            bronze_geolocation_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            geolocation_zip_code_prefix INT UNSIGNED NULL,
            geolocation_lat DECIMAL(10,7) NULL,
            geolocation_lng DECIMAL(10,7) NULL,
            geolocation_city VARCHAR(100) NULL,
            geolocation_state CHAR(2) NULL,
            PRIMARY KEY (bronze_geolocation_id),
            KEY idx_bronze_geolocation_zip (geolocation_zip_code_prefix)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "bronze_orders": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "bronze_order_items": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "bronze_order_payments": """
        CREATE TABLE IF NOT EXISTS bronze_order_payments (
            order_id CHAR(32) NOT NULL,
            payment_sequential INT UNSIGNED NOT NULL,
            payment_type VARCHAR(32) NULL,
            payment_installments INT UNSIGNED NULL,
            payment_value DECIMAL(14,2) NOT NULL,
            PRIMARY KEY (order_id, payment_sequential),
            KEY idx_bronze_order_payments_order (order_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "bronze_order_reviews": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "bronze_products": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "bronze_sellers": """
        CREATE TABLE IF NOT EXISTS bronze_sellers (
            seller_id CHAR(32) NOT NULL,
            seller_zip_code_prefix INT UNSIGNED NULL,
            seller_city VARCHAR(100) NULL,
            seller_state CHAR(2) NULL,
            PRIMARY KEY (seller_id),
            KEY idx_bronze_sellers_zip (seller_zip_code_prefix)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "bronze_translation": """
        CREATE TABLE IF NOT EXISTS bronze_translation (
            product_category_name VARCHAR(150) NOT NULL,
            product_category_name_english VARCHAR(150) NULL,
            PRIMARY KEY (product_category_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "silver_geolocations": """
        CREATE TABLE IF NOT EXISTS silver_geolocations (
            zip_code_prefix INT UNSIGNED NOT NULL,
            city VARCHAR(100) NULL,
            state CHAR(2) NULL,
            latitude DECIMAL(10,7) NULL,
            longitude DECIMAL(10,7) NULL,
            PRIMARY KEY (zip_code_prefix)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "silver_product_categories": """
        CREATE TABLE IF NOT EXISTS silver_product_categories (
            product_category_name VARCHAR(150) NOT NULL,
            product_category_name_english VARCHAR(150) NULL,
            PRIMARY KEY (product_category_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "silver_customers": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "silver_sellers": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "silver_products": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "silver_orders": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "silver_order_items": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "silver_order_payments": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "silver_order_reviews": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "gold_dim_customers": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "gold_dim_products": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "gold_dim_sellers": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "gold_fct_orders": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "gold_fct_order_items": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "gold_agg_sales_monthly": """
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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
}


def _normalize_value(value: Any) -> Any:
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            return value

    return value


def create_layered_schema(connection: Connection) -> None:
    for table_name in LAYER_TABLE_ORDER:
        connection.execute(text(TABLE_DDL[table_name]))


def clear_layered_tables(connection: Connection) -> None:
    for table_name in LAYER_DELETE_ORDER:
        connection.execute(text(f"DELETE FROM `{table_name}`"))


def load_dataframe(
    connection: Connection,
    table_name: str,
    dataframe: pd.DataFrame,
    batch_size: int = 1000,
) -> None:
    columns = TABLE_COLUMNS[table_name]
    ordered_dataframe = dataframe.loc[:, columns]
    columns_sql = ", ".join(f"`{column}`" for column in columns)
    values_sql = ", ".join(f":{column}" for column in columns)
    statement = text(
        f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({values_sql})"
    )

    for start in range(0, len(ordered_dataframe), batch_size):
        chunk = ordered_dataframe.iloc[start:start + batch_size]
        records = [
            {column: _normalize_value(value) for column, value in row.items()}
            for row in chunk.to_dict(orient="records")
        ]

        if records:
            connection.execute(statement, records)
