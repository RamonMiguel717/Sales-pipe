from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from core import paths


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


DDL_FILE_ORDER = [
    paths.BASE / "sql" / "ddl" / "bronze.sql",
    paths.BASE / "sql" / "ddl" / "silver.sql",
    paths.BASE / "sql" / "ddl" / "gold.sql",
]


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


def _read_sql_statements(file_path: Path) -> list[str]:
    if not file_path.exists():
        raise FileNotFoundError(f"Arquivo DDL nao encontrado: {file_path}")

    sql_content = file_path.read_text(encoding="utf-8")
    return [
        statement.strip()
        for statement in sql_content.split(";")
        if statement.strip()
    ]


def create_layered_schema(connection: Connection) -> None:
    for file_path in DDL_FILE_ORDER:
        for statement in _read_sql_statements(file_path):
            connection.exec_driver_sql(statement)


def clear_layered_tables(connection: Connection) -> None:
    for table_name in LAYER_DELETE_ORDER:
        connection.exec_driver_sql(f"DELETE FROM `{table_name}`")


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
    statement = (
        f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({values_sql})"
    )
    compiled_statement = text(statement)

    for start in range(0, len(ordered_dataframe), batch_size):
        chunk = ordered_dataframe.iloc[start:start + batch_size]
        records = [
            {column: _normalize_value(value) for column, value in row.items()}
            for row in chunk.to_dict(orient="records")
        ]

        if records:
            connection.execute(compiled_statement, records)
