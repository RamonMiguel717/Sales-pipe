from pathlib import Path
import sqlite3

import pandas as pd

from core import paths
from core.database import get_mysql_engine
from core.getters import Getters


TABLE_FILES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "translation": "product_category_name_translation.csv",
}


TABLE_RELATIONSHIPS = {
    "orders": {"customer_id": ("customers", "customer_id")},
    "order_items": {
        "order_id": ("orders", "order_id"),
        "product_id": ("products", "product_id"),
        "seller_id": ("sellers", "seller_id"),
    },
    "order_payments": {"order_id": ("orders", "order_id")},
    "order_reviews": {"order_id": ("orders", "order_id")},
    "products": {"product_category_name": ("translation", "product_category_name")},
    "customers": {"customer_zip_code_prefix": ("geolocation", "geolocation_zip_code_prefix")},
    "sellers": {"seller_zip_code_prefix": ("geolocation", "geolocation_zip_code_prefix")},
}


DATETIME_COLUMNS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items": ["shipping_limit_date"],
    "order_reviews": ["review_creation_date", "review_answer_timestamp"],
}


ANALYTICS_PARSE_DATES = {
    "fct_orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "fct_order_items": [
        "shipping_limit_date",
        "order_purchase_timestamp",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
}


ANALYTICS_BOOL_COLUMNS = {
    "fct_orders": ["is_delivered_late", "has_review_comment"],
    "fct_order_items": ["is_delivered_late"],
}


# Para CEP, latitude e longitude funcionam melhor como centroide médio;
# para cidade e estado, a moda é mais consistente por serem variáveis categóricas.
SQL_TRANSFORM_SCRIPT = """
CREATE TEMP TABLE geolocation_prepared AS
WITH geolocation_centroid AS (
    SELECT
        geolocation_zip_code_prefix,
        AVG(geolocation_lat) AS geolocation_lat,
        AVG(geolocation_lng) AS geolocation_lng
    FROM geolocation
    GROUP BY geolocation_zip_code_prefix
),
geolocation_mode AS (
    SELECT
        geolocation_zip_code_prefix,
        geolocation_city,
        geolocation_state
    FROM (
        SELECT
            geolocation_zip_code_prefix,
            geolocation_city,
            geolocation_state,
            COUNT(*) AS occurrences,
            ROW_NUMBER() OVER (
                PARTITION BY geolocation_zip_code_prefix
                ORDER BY COUNT(*) DESC, geolocation_city, geolocation_state
            ) AS row_num
        FROM geolocation
        GROUP BY
            geolocation_zip_code_prefix,
            geolocation_city,
            geolocation_state
    )
    WHERE row_num = 1
)
SELECT
    centroid.geolocation_zip_code_prefix,
    centroid.geolocation_lat,
    centroid.geolocation_lng,
    mode.geolocation_city,
    mode.geolocation_state
FROM geolocation_centroid AS centroid
LEFT JOIN geolocation_mode AS mode
    ON centroid.geolocation_zip_code_prefix = mode.geolocation_zip_code_prefix;

CREATE TEMP TABLE payments_prepared AS
WITH payment_type_rank AS (
    SELECT
        order_id,
        payment_type,
        COUNT(*) AS payment_type_count,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY COUNT(*) DESC, payment_type
        ) AS row_num
    FROM order_payments
    GROUP BY order_id, payment_type
)
SELECT
    payments.order_id,
    SUM(payments.payment_value) AS payment_value_total,
    MAX(payments.payment_sequential) AS payment_sequential_max,
    MAX(payments.payment_installments) AS payment_installments_max,
    AVG(payments.payment_installments) AS payment_installments_mean,
    COUNT(DISTINCT payments.payment_type) AS payment_types_nunique,
    payment_type_rank.payment_type AS payment_type_main
FROM order_payments AS payments
LEFT JOIN payment_type_rank
    ON payments.order_id = payment_type_rank.order_id
   AND payment_type_rank.row_num = 1
GROUP BY payments.order_id, payment_type_rank.payment_type;

CREATE TEMP TABLE reviews_prepared AS
SELECT
    order_id,
    COUNT(DISTINCT review_id) AS review_count,
    AVG(review_score) AS review_score_mean,
    MIN(review_score) AS review_score_min,
    MAX(review_score) AS review_score_max,
    MAX(
        CASE
            WHEN review_comment_title IS NOT NULL OR review_comment_message IS NOT NULL THEN 1
            ELSE 0
        END
    ) AS has_review_comment
FROM order_reviews
GROUP BY order_id;

CREATE TEMP TABLE order_items_prepared AS
SELECT
    *,
    price + freight_value AS item_total_value
FROM order_items;

CREATE TEMP TABLE order_items_summary AS
SELECT
    order_id,
    COUNT(order_item_id) AS items_count,
    COUNT(DISTINCT product_id) AS products_count,
    COUNT(DISTINCT seller_id) AS sellers_count,
    SUM(price) AS items_value,
    SUM(freight_value) AS freight_value,
    SUM(item_total_value) AS order_total_value
FROM order_items_prepared
GROUP BY order_id;

CREATE TEMP TABLE dim_customers_sql AS
SELECT
    customers.*,
    geolocation_prepared.geolocation_lat AS customer_lat,
    geolocation_prepared.geolocation_lng AS customer_lng,
    geolocation_prepared.geolocation_city AS customer_geo_city,
    geolocation_prepared.geolocation_state AS customer_geo_state
FROM customers
LEFT JOIN geolocation_prepared
    ON customers.customer_zip_code_prefix = geolocation_prepared.geolocation_zip_code_prefix;

CREATE TEMP TABLE dim_sellers_sql AS
SELECT
    sellers.*,
    geolocation_prepared.geolocation_lat AS seller_lat,
    geolocation_prepared.geolocation_lng AS seller_lng,
    geolocation_prepared.geolocation_city AS seller_geo_city,
    geolocation_prepared.geolocation_state AS seller_geo_state
FROM sellers
LEFT JOIN geolocation_prepared
    ON sellers.seller_zip_code_prefix = geolocation_prepared.geolocation_zip_code_prefix;

CREATE TEMP TABLE dim_products_sql AS
SELECT
    products.*,
    translation.product_category_name_english
FROM products
LEFT JOIN translation
    ON products.product_category_name = translation.product_category_name;

CREATE TEMP TABLE fct_orders_sql AS
SELECT
    orders.*,
    order_items_summary.items_count,
    order_items_summary.products_count,
    order_items_summary.sellers_count,
    order_items_summary.items_value,
    order_items_summary.freight_value,
    order_items_summary.order_total_value,
    payments_prepared.payment_value_total,
    payments_prepared.payment_sequential_max,
    payments_prepared.payment_installments_max,
    payments_prepared.payment_installments_mean,
    payments_prepared.payment_types_nunique,
    payments_prepared.payment_type_main,
    reviews_prepared.review_count,
    reviews_prepared.review_score_mean,
    reviews_prepared.review_score_min,
    reviews_prepared.review_score_max,
    reviews_prepared.has_review_comment,
    (julianday(orders.order_approved_at) - julianday(orders.order_purchase_timestamp)) * 24 AS approve_hours,
    julianday(orders.order_delivered_customer_date) - julianday(orders.order_purchase_timestamp) AS delivery_days,
    julianday(orders.order_delivered_customer_date) - julianday(orders.order_delivered_carrier_date) AS carrier_to_customer_days,
    julianday(orders.order_estimated_delivery_date) - julianday(orders.order_purchase_timestamp) AS estimated_delivery_days,
    julianday(orders.order_delivered_customer_date) - julianday(orders.order_estimated_delivery_date) AS delivery_delay_days,
    CASE
        WHEN orders.order_delivered_customer_date IS NULL OR orders.order_estimated_delivery_date IS NULL THEN NULL
        WHEN julianday(orders.order_delivered_customer_date) > julianday(orders.order_estimated_delivery_date) THEN 1
        ELSE 0
    END AS is_delivered_late,
    CAST(strftime('%Y', orders.order_purchase_timestamp) AS INTEGER) AS purchase_year,
    CAST(strftime('%m', orders.order_purchase_timestamp) AS INTEGER) AS purchase_month,
    strftime('%Y-%m', orders.order_purchase_timestamp) AS purchase_year_month
FROM orders
LEFT JOIN order_items_summary
    ON orders.order_id = order_items_summary.order_id
LEFT JOIN payments_prepared
    ON orders.order_id = payments_prepared.order_id
LEFT JOIN reviews_prepared
    ON orders.order_id = reviews_prepared.order_id;

CREATE TEMP TABLE fct_order_items_sql AS
SELECT
    order_items_prepared.*,
    fct_orders_sql.customer_id,
    fct_orders_sql.order_status,
    fct_orders_sql.order_purchase_timestamp,
    fct_orders_sql.order_delivered_carrier_date,
    fct_orders_sql.order_delivered_customer_date,
    fct_orders_sql.order_estimated_delivery_date,
    fct_orders_sql.purchase_year,
    fct_orders_sql.purchase_month,
    fct_orders_sql.purchase_year_month,
    fct_orders_sql.delivery_days,
    fct_orders_sql.delivery_delay_days,
    fct_orders_sql.is_delivered_late,
    julianday(order_items_prepared.shipping_limit_date) - julianday(fct_orders_sql.order_delivered_carrier_date) AS shipping_accuracy_days
FROM order_items_prepared
LEFT JOIN fct_orders_sql
    ON order_items_prepared.order_id = fct_orders_sql.order_id;

CREATE TEMP TABLE agg_sales_monthly_sql AS
SELECT
    purchase_year,
    purchase_month,
    purchase_year_month,
    COUNT(DISTINCT order_id) AS orders_count,
    COUNT(DISTINCT customer_id) AS customers_count,
    SUM(order_total_value) AS revenue_total,
    SUM(payment_value_total) AS payment_total,
    SUM(freight_value) AS freight_total,
    AVG(order_total_value) AS avg_ticket,
    AVG(freight_value) AS avg_freight,
    AVG(delivery_days) AS avg_delivery_days,
    AVG(review_score_mean) AS avg_review_score,
    SUM(CASE WHEN is_delivered_late IS NOT NULL THEN 1 ELSE 0 END) AS delivered_orders,
    SUM(COALESCE(is_delivered_late, 0)) AS late_orders,
    CASE
        WHEN SUM(CASE WHEN is_delivered_late IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN NULL
        ELSE 1 - (
            SUM(COALESCE(is_delivered_late, 0)) * 1.0
            / SUM(CASE WHEN is_delivered_late IS NOT NULL THEN 1 ELSE 0 END)
        )
    END AS otd_rate
FROM fct_orders_sql
GROUP BY purchase_year, purchase_month, purchase_year_month;
"""


ANALYTICS_QUERIES = {
    "dim_customers": "SELECT * FROM dim_customers_sql",
    "dim_products": "SELECT * FROM dim_products_sql",
    "dim_sellers": "SELECT * FROM dim_sellers_sql",
    "fct_orders": "SELECT * FROM fct_orders_sql",
    "fct_order_items": "SELECT * FROM fct_order_items_sql",
    "agg_sales_monthly": "SELECT * FROM agg_sales_monthly_sql",
}


SQL_INDEX_SCRIPT = """
CREATE INDEX idx_orders_order_id ON orders(order_id);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_order_items_seller_id ON order_items(seller_id);
CREATE INDEX idx_order_payments_order_id ON order_payments(order_id);
CREATE INDEX idx_order_reviews_order_id ON order_reviews(order_id);
CREATE INDEX idx_customers_customer_id ON customers(customer_id);
CREATE INDEX idx_customers_zip ON customers(customer_zip_code_prefix);
CREATE INDEX idx_sellers_seller_id ON sellers(seller_id);
CREATE INDEX idx_sellers_zip ON sellers(seller_zip_code_prefix);
CREATE INDEX idx_products_product_id ON products(product_id);
CREATE INDEX idx_products_category ON products(product_category_name);
CREATE INDEX idx_translation_category ON translation(product_category_name);
CREATE INDEX idx_geolocation_zip ON geolocation(geolocation_zip_code_prefix);
"""


def load_raw_tables(entry_path: str = paths.ENTRADA) -> dict[str, pd.DataFrame]:
    getters = Getters()
    base_path = Path(entry_path)

    missing_files = [
        filename for filename in TABLE_FILES.values()
        if not (base_path / filename).exists()
    ]
    if missing_files:
        raise FileNotFoundError(
            f"Arquivos obrigatórios não encontrados em {base_path}: {missing_files}"
        )

    tables = {
        name: getters.load_data(str(base_path / filename))
        for name, filename in TABLE_FILES.items()
    }

    for table_name, columns in DATETIME_COLUMNS.items():
        for column in columns:
            tables[table_name][column] = pd.to_datetime(
                tables[table_name][column],
                errors="coerce",
            )

    return tables


def _create_sql_connection(raw_tables: dict[str, pd.DataFrame]) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")

    for table_name, dataframe in raw_tables.items():
        dataframe.to_sql(table_name, connection, index=False, if_exists="replace")

    connection.executescript(SQL_INDEX_SCRIPT)
    connection.executescript(SQL_TRANSFORM_SCRIPT)
    return connection


def _load_sql_model(connection: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    dataframe = pd.read_sql_query(
        ANALYTICS_QUERIES[table_name],
        connection,
        parse_dates=ANALYTICS_PARSE_DATES.get(table_name, None),
    )

    for column in ANALYTICS_BOOL_COLUMNS.get(table_name, []):
        dataframe[column] = dataframe[column].astype("boolean")

    return dataframe


def build_analytics_tables(entry_path: str = paths.ENTRADA) -> dict[str, pd.DataFrame]:
    raw_tables = load_raw_tables(entry_path)

    with _create_sql_connection(raw_tables) as connection:
        analytics_tables = {
            table_name: _load_sql_model(connection, table_name)
            for table_name in ANALYTICS_QUERIES
        }

    tables = {f"raw_{name}": dataframe for name, dataframe in raw_tables.items()}
    tables.update(analytics_tables)

    return tables


def get_relationships() -> dict[str, dict[str, tuple[str, str]]]:
    return TABLE_RELATIONSHIPS


def load_mysql_tables(tables: dict[str, pd.DataFrame], if_exists: str = "replace") -> None:
    engine = get_mysql_engine()

    with engine.begin() as connection:
        for table_name, dataframe in tables.items():
            dataframe.to_sql(
                table_name,
                connection,
                index=False,
                if_exists=if_exists,
                chunksize=1000,
                method="multi",
            )


def principal(
    entry_path: str = paths.ENTRADA,
    persist_mysql: bool = False,
    if_exists: str = "replace",
) -> dict[str, pd.DataFrame]:
    tables = build_analytics_tables(entry_path)

    if persist_mysql:
        load_mysql_tables(tables, if_exists=if_exists)

    return tables
