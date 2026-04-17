import logging

import pandas as pd


logger = logging.getLogger(__name__)


def validate_no_nulls(
    df: pd.DataFrame,
    columns: list[str],
    table_name: str,
    raise_on_error: bool = False,
) -> bool:
    """Validate that the selected columns do not contain null values."""
    has_nulls = False

    for column in columns:
        if column not in df.columns:
            logger.warning("[%s] Column '%s' not found", table_name, column)
            has_nulls = True
            continue

        null_count = int(df[column].isna().sum())
        if null_count > 0:
            logger.warning(
                "[%s] Column '%s' has %d null value(s) (%.2f%%)",
                table_name,
                column,
                null_count,
                null_count / len(df) * 100,
            )
            has_nulls = True

    if has_nulls and raise_on_error:
        raise ValueError(f"[{table_name}] Validation failed: columns with null values.")

    return not has_nulls


def validate_row_count(
    df: pd.DataFrame,
    min_rows: int,
    table_name: str,
    raise_on_error: bool = True,
) -> bool:
    """Validate the minimum row count of a DataFrame."""
    actual_rows = len(df)
    if actual_rows < min_rows:
        message = (
            f"[{table_name}] Expected at least {min_rows} rows, found {actual_rows}."
        )
        if raise_on_error:
            raise ValueError(message)
        logger.warning(message)
        return False

    logger.debug("[%s] Row count OK: %d >= %d", table_name, actual_rows, min_rows)
    return True


def validate_no_duplicates(
    df: pd.DataFrame,
    key_columns: list[str],
    table_name: str,
    raise_on_error: bool = False,
) -> bool:
    """Validate that the provided key columns are unique."""
    existing_columns = [column for column in key_columns if column in df.columns]
    if not existing_columns:
        logger.warning(
            "[%s] None of the key columns were found: %s",
            table_name,
            key_columns,
        )
        return False

    duplicate_count = int(df.duplicated(subset=existing_columns).sum())
    if duplicate_count > 0:
        message = (
            f"[{table_name}] Found {duplicate_count} duplicated row(s) on keys "
            f"{existing_columns}"
        )
        if raise_on_error:
            raise ValueError(message)
        logger.warning(message)
        return False

    logger.debug("[%s] No duplicates found on keys %s", table_name, existing_columns)
    return True


def validate_value_range(
    df: pd.DataFrame,
    column: str,
    min_value: float | None,
    max_value: float | None,
    table_name: str,
    raise_on_error: bool = False,
) -> bool:
    """Validate that a numeric column stays within the expected range."""
    if column not in df.columns:
        logger.warning("[%s] Column '%s' not found", table_name, column)
        return False

    series = df[column].dropna()
    out_of_range = pd.Series(False, index=series.index)

    if min_value is not None:
        out_of_range = out_of_range | (series < min_value)
    if max_value is not None:
        out_of_range = out_of_range | (series > max_value)

    count = int(out_of_range.sum())
    if count > 0:
        message = (
            f"[{table_name}] Column '{column}' has {count} value(s) outside the "
            f"range [{min_value}, {max_value}]"
        )
        if raise_on_error:
            raise ValueError(message)
        logger.warning(message)
        return False

    return True


def run_bronze_validations(tables: dict[str, pd.DataFrame]) -> None:
    """Run lightweight quality checks on the Bronze layer."""
    validate_row_count(tables["bronze_orders"], min_rows=1000, table_name="bronze_orders")
    validate_row_count(
        tables["bronze_order_items"],
        min_rows=1000,
        table_name="bronze_order_items",
    )
    validate_no_duplicates(tables["bronze_orders"], ["order_id"], "bronze_orders")
    validate_no_duplicates(tables["bronze_customers"], ["customer_id"], "bronze_customers")


def run_silver_validations(tables: dict[str, pd.DataFrame]) -> None:
    """Run lightweight quality checks on the Silver layer."""
    validate_no_nulls(
        tables["silver_orders"],
        ["order_id", "customer_id", "order_purchase_timestamp"],
        "silver_orders",
    )
    validate_no_nulls(
        tables["silver_order_items"],
        ["order_id", "product_id", "seller_id", "price"],
        "silver_order_items",
    )
    validate_value_range(
        tables["silver_order_items"],
        "price",
        min_value=0,
        max_value=None,
        table_name="silver_order_items",
    )
    validate_value_range(
        tables["silver_order_items"],
        "freight_value",
        min_value=0,
        max_value=None,
        table_name="silver_order_items",
    )


def run_gold_validations(tables: dict[str, pd.DataFrame]) -> None:
    """Run lightweight quality checks on the Gold layer."""
    validate_row_count(tables["gold_fct_orders"], min_rows=1000, table_name="gold_fct_orders")
    validate_no_duplicates(tables["gold_fct_orders"], ["order_id"], "gold_fct_orders")
    validate_value_range(
        tables["gold_fct_orders"],
        "order_total_value",
        min_value=0,
        max_value=None,
        table_name="gold_fct_orders",
    )
    validate_value_range(
        tables["gold_agg_sales_monthly"],
        "otd_rate",
        min_value=0,
        max_value=1,
        table_name="gold_agg_sales_monthly",
    )
