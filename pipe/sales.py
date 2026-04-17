import logging
from pathlib import Path
import sqlite3

import pandas as pd

from core import paths
from core.database import get_mysql_engine
from core.getters import load_data
from core.mysql_schema import (
    BRONZE_TABLE_ORDER,
    GOLD_TABLE_ORDER,
    LAYER_TABLE_ORDER,
    SILVER_TABLE_ORDER,
    clear_layered_tables,
    create_layered_schema,
    load_dataframe,
)
from core.validators import (
    run_bronze_validations,
    run_gold_validations,
    run_silver_validations,
)


logger = logging.getLogger(__name__)
PathLike = str | Path


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
    "customers": {
        "customer_zip_code_prefix": ("geolocation", "geolocation_zip_code_prefix")
    },
    "sellers": {
        "seller_zip_code_prefix": ("geolocation", "geolocation_zip_code_prefix")
    },
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


GOLD_PARSE_DATES = {
    "gold_fct_orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "gold_fct_order_items": [
        "shipping_limit_date",
        "order_purchase_timestamp",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
}


GOLD_BOOL_COLUMNS = {
    "gold_fct_orders": ["is_delivered_late", "has_review_comment"],
    "gold_fct_order_items": ["is_delivered_late"],
}


SQL_TRANSFORM_SCRIPT = paths.BASE / "sql" / "transform.sql"


GOLD_QUERIES = {
    "gold_dim_customers": "SELECT * FROM gold_dim_customers_sql",
    "gold_dim_products": "SELECT * FROM gold_dim_products_sql",
    "gold_dim_sellers": "SELECT * FROM gold_dim_sellers_sql",
    "gold_fct_orders": "SELECT * FROM gold_fct_orders_sql",
    "gold_fct_order_items": "SELECT * FROM gold_fct_order_items_sql",
    "gold_agg_sales_monthly": "SELECT * FROM gold_agg_sales_monthly_sql",
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
CREATE INDEX idx_product_categories_category ON product_categories(product_category_name);
CREATE INDEX idx_geolocations_zip ON geolocations(zip_code_prefix);
"""


def _mode_or_first(series: pd.Series) -> object:
    """Return the mode of a series after dropping null values."""
    series = series.dropna()
    if series.empty:
        return pd.NA
    return series.mode().iloc[0]


def load_raw_tables(entry_path: PathLike = paths.ENTRADA) -> dict[str, pd.DataFrame]:
    """Load the source CSV tables from the datalake."""
    base_path = Path(entry_path)
    logger.info("Loading raw tables from '%s'", base_path)

    missing_files = [
        filename for filename in TABLE_FILES.values() if not (base_path / filename).exists()
    ]
    if missing_files:
        raise FileNotFoundError(
            f"Arquivos obrigatorios nao encontrados em '{base_path}': {missing_files}"
        )

    tables = {
        name: load_data(base_path / filename)
        for name, filename in TABLE_FILES.items()
    }

    for table_name, columns in DATETIME_COLUMNS.items():
        for column in columns:
            tables[table_name][column] = pd.to_datetime(
                tables[table_name][column],
                errors="coerce",
            )

    logger.info("Raw tables loaded: %s", list(tables.keys()))
    return tables


def _prepare_bronze_tables(
    raw_tables: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """Replicate the raw source tables into the Bronze layer."""
    bronze_tables = {
        f"bronze_{table_name}": dataframe.copy()
        for table_name, dataframe in raw_tables.items()
    }
    logger.debug("Prepared %d bronze tables", len(bronze_tables))
    return bronze_tables


def _prepare_silver_tables(
    raw_tables: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """Apply cleansing and standardization rules to build the Silver layer."""
    geolocations = (
        raw_tables["geolocation"]
        .groupby("geolocation_zip_code_prefix", as_index=False)
        .agg(
            latitude=("geolocation_lat", "mean"),
            longitude=("geolocation_lng", "mean"),
            city=("geolocation_city", _mode_or_first),
            state=("geolocation_state", _mode_or_first),
        )
        .rename(columns={"geolocation_zip_code_prefix": "zip_code_prefix"})
    )

    geolocations["latitude"] = geolocations["latitude"].round(7)
    geolocations["longitude"] = geolocations["longitude"].round(7)

    order_items = raw_tables["order_items"].copy()
    order_payments = raw_tables["order_payments"].copy()
    order_items["price"] = order_items["price"].round(2)
    order_items["freight_value"] = order_items["freight_value"].round(2)
    order_payments["payment_value"] = order_payments["payment_value"].round(2)

    silver_tables = {
        "silver_geolocations": geolocations,
        "silver_product_categories": raw_tables["translation"].copy(),
        "silver_customers": raw_tables["customers"].copy(),
        "silver_sellers": raw_tables["sellers"].copy(),
        "silver_products": raw_tables["products"].copy(),
        "silver_orders": raw_tables["orders"].copy(),
        "silver_order_items": order_items,
        "silver_order_payments": order_payments,
        "silver_order_reviews": raw_tables["order_reviews"].copy(),
    }

    logger.debug("Prepared %d silver tables", len(silver_tables))
    return silver_tables


def _load_transform_sql() -> str:
    """Read the external SQL transformation script."""
    if not SQL_TRANSFORM_SCRIPT.exists():
        raise FileNotFoundError(f"Arquivo SQL nao encontrado: {SQL_TRANSFORM_SCRIPT}")

    return SQL_TRANSFORM_SCRIPT.read_text(encoding="utf-8")


def _create_sql_connection(sql_tables: dict[str, pd.DataFrame]) -> sqlite3.Connection:
    """Create an in-memory SQLite database and materialize the Gold transformations."""
    connection = sqlite3.connect(":memory:")

    for table_name, dataframe in sql_tables.items():
        dataframe.to_sql(table_name, connection, index=False, if_exists="replace")

    connection.executescript(SQL_INDEX_SCRIPT)
    connection.executescript(_load_transform_sql())
    return connection


def _load_sql_model(connection: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    """Load a Gold table from SQLite and restore the expected pandas dtypes."""
    dataframe = pd.read_sql_query(
        GOLD_QUERIES[table_name],
        connection,
        parse_dates=GOLD_PARSE_DATES.get(table_name),
    )

    for column in GOLD_BOOL_COLUMNS.get(table_name, []):
        dataframe[column] = dataframe[column].astype("boolean")

    return dataframe


def _prepare_gold_tables(
    silver_tables: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """Run the SQL transformations that build the Gold layer."""
    sql_tables = {
        table_name.removeprefix("silver_"): dataframe
        for table_name, dataframe in silver_tables.items()
    }

    logger.info("Building Gold tables via in-memory SQLite")
    with _create_sql_connection(sql_tables) as connection:
        gold_tables = {
            table_name: _load_sql_model(connection, table_name)
            for table_name in GOLD_QUERIES
        }

    logger.info("Prepared %d gold tables", len(gold_tables))
    return gold_tables


def build_bronze_tables(entry_path: PathLike = paths.ENTRADA) -> dict[str, pd.DataFrame]:
    """Return the Bronze layer."""
    return _prepare_bronze_tables(load_raw_tables(entry_path))


def build_silver_tables(entry_path: PathLike = paths.ENTRADA) -> dict[str, pd.DataFrame]:
    """Return the Silver layer."""
    return _prepare_silver_tables(load_raw_tables(entry_path))


def build_gold_tables(entry_path: PathLike = paths.ENTRADA) -> dict[str, pd.DataFrame]:
    """Return the Gold analytical layer."""
    raw_tables = load_raw_tables(entry_path)
    silver_tables = _prepare_silver_tables(raw_tables)
    return _prepare_gold_tables(silver_tables)


def build_elt_tables(entry_path: PathLike = paths.ENTRADA) -> dict[str, pd.DataFrame]:
    """Build the Bronze, Silver and Gold layers in memory."""
    logger.info("Starting full ELT build")
    raw_tables = load_raw_tables(entry_path)

    bronze_tables = _prepare_bronze_tables(raw_tables)
    run_bronze_validations(bronze_tables)

    silver_tables = _prepare_silver_tables(raw_tables)
    run_silver_validations(silver_tables)

    gold_tables = _prepare_gold_tables(silver_tables)
    run_gold_validations(gold_tables)

    all_tables = {**bronze_tables, **silver_tables, **gold_tables}
    logger.info("ELT build finished with %d tables", len(all_tables))
    return all_tables


def build_analytics_tables(entry_path: PathLike = paths.ENTRADA) -> dict[str, pd.DataFrame]:
    """Compatibility alias that returns the Gold analytical layer."""
    return build_gold_tables(entry_path)


def build_3nf_tables(entry_path: PathLike = paths.ENTRADA) -> dict[str, pd.DataFrame]:
    """Compatibility alias that returns the Silver relational layer without prefixes."""
    silver_tables = build_silver_tables(entry_path)
    return {
        table_name.removeprefix("silver_"): dataframe
        for table_name, dataframe in silver_tables.items()
    }


def get_relationships() -> dict[str, dict[str, tuple[str, str]]]:
    """Expose the mapped business keys across the source tables."""
    return TABLE_RELATIONSHIPS


def create_mysql_layered_schema() -> None:
    """Create the Bronze, Silver and Gold schema in MySQL."""
    engine = get_mysql_engine()
    with engine.begin() as connection:
        create_layered_schema(connection)


def create_mysql_3nf_schema() -> None:
    """Compatibility alias for creating the MySQL schema."""
    create_mysql_layered_schema()


def load_mysql_elt_tables(
    entry_path: PathLike = paths.ENTRADA,
    batch_size: int = 1000,
) -> dict[str, pd.DataFrame]:
    """Persist the full ELT stack into MySQL and return the Gold layer."""
    tables = build_elt_tables(entry_path)
    engine = get_mysql_engine()

    logger.info("Starting MySQL load")
    with engine.begin() as connection:
        create_layered_schema(connection)
        clear_layered_tables(connection)

        for table_name in LAYER_TABLE_ORDER:
            load_dataframe(
                connection,
                table_name,
                tables[table_name],
                batch_size=batch_size,
            )

    logger.info("MySQL load finished")
    return {table_name: tables[table_name] for table_name in GOLD_TABLE_ORDER}


def load_mysql_3nf_tables(
    entry_path: PathLike = paths.ENTRADA,
    batch_size: int = 1000,
) -> dict[str, pd.DataFrame]:
    """Persist the Bronze and Silver layers into MySQL."""
    raw_tables = load_raw_tables(entry_path)
    bronze_tables = _prepare_bronze_tables(raw_tables)
    silver_tables = _prepare_silver_tables(raw_tables)
    engine = get_mysql_engine()

    with engine.begin() as connection:
        create_layered_schema(connection)
        clear_layered_tables(connection)

        for table_name in BRONZE_TABLE_ORDER:
            load_dataframe(
                connection,
                table_name,
                bronze_tables[table_name],
                batch_size=batch_size,
            )

        for table_name in SILVER_TABLE_ORDER:
            load_dataframe(
                connection,
                table_name,
                silver_tables[table_name],
                batch_size=batch_size,
            )

    return {
        table_name.removeprefix("silver_"): dataframe
        for table_name, dataframe in silver_tables.items()
    }


def principal(
    entry_path: PathLike = paths.ENTRADA,
    persist_mysql: bool = False,
    batch_size: int = 1000,
) -> dict[str, pd.DataFrame]:
    """Run the pipeline and return the Gold layer."""
    if persist_mysql:
        return load_mysql_elt_tables(entry_path, batch_size=batch_size)

    return build_gold_tables(entry_path)


def run_pipeline(
    entry_path: PathLike = paths.ENTRADA,
    persist_mysql: bool = False,
    batch_size: int = 1000,
) -> dict[str, pd.DataFrame]:
    """English alias for principal()."""
    return principal(
        entry_path=entry_path,
        persist_mysql=persist_mysql,
        batch_size=batch_size,
    )
