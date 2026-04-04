from pathlib import Path

import pandas as pd

from core import paths
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


def load_sql_tables(entry_path: str = paths.ENTRADA) -> dict[str, pd.DataFrame]:
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


def get_relationships() -> dict[str, dict[str, tuple[str, str]]]:
    return TABLE_RELATIONSHIPS


def principal(entry_path: str = paths.ENTRADA) -> dict[str, pd.DataFrame]:
    return load_sql_tables(entry_path)
