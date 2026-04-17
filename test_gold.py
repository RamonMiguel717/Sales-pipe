import pandas as pd
import pytest

from pipe.sales import _prepare_gold_tables, _prepare_silver_tables


@pytest.fixture
def gold_tables(raw_tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    silver_tables = _prepare_silver_tables(raw_tables)
    return _prepare_gold_tables(silver_tables)


class TestGoldFctOrders:
    def test_returns_orders_fact_table(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        assert "gold_fct_orders" in gold_tables

    def test_order_count(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        assert len(gold_tables["gold_fct_orders"]) == 4

    def test_on_time_order(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        orders = gold_tables["gold_fct_orders"]
        order = orders[orders["order_id"] == "order_001"].iloc[0]
        assert order["is_delivered_late"] == False  # noqa: E712

    def test_late_order(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        orders = gold_tables["gold_fct_orders"]
        order = orders[orders["order_id"] == "order_002"].iloc[0]
        assert order["is_delivered_late"] == True  # noqa: E712

    def test_open_order_stays_null(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        orders = gold_tables["gold_fct_orders"]
        order = orders[orders["order_id"] == "order_003"].iloc[0]
        assert pd.isna(order["is_delivered_late"])

    def test_delivery_days_is_null_for_open_order(
        self, gold_tables: dict[str, pd.DataFrame]
    ) -> None:
        orders = gold_tables["gold_fct_orders"]
        order = orders[orders["order_id"] == "order_003"].iloc[0]
        assert pd.isna(order["delivery_days"])

    def test_delivery_days_positive_for_delivered_orders(
        self, gold_tables: dict[str, pd.DataFrame]
    ) -> None:
        orders = gold_tables["gold_fct_orders"]
        delivered = orders[orders["order_id"].isin(["order_001", "order_002", "order_004"])]
        assert (delivered["delivery_days"].dropna() > 0).all()

    def test_late_order_has_positive_delay(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        orders = gold_tables["gold_fct_orders"]
        order = orders[orders["order_id"] == "order_002"].iloc[0]
        assert order["delivery_delay_days"] > 0

    def test_on_time_order_has_non_positive_delay(
        self, gold_tables: dict[str, pd.DataFrame]
    ) -> None:
        orders = gold_tables["gold_fct_orders"]
        order = orders[orders["order_id"] == "order_001"].iloc[0]
        assert order["delivery_delay_days"] <= 0

    def test_items_count_is_correct(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        orders = gold_tables["gold_fct_orders"]
        order_001 = orders[orders["order_id"] == "order_001"].iloc[0]
        order_002 = orders[orders["order_id"] == "order_002"].iloc[0]
        assert order_001["items_count"] == 2
        assert order_002["items_count"] == 1

    def test_order_total_value(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        orders = gold_tables["gold_fct_orders"]
        order_001 = orders[orders["order_id"] == "order_001"].iloc[0]
        assert abs(order_001["order_total_value"] - 165.0) < 0.01

    def test_multiple_payment_types(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        orders = gold_tables["gold_fct_orders"]
        order_002 = orders[orders["order_id"] == "order_002"].iloc[0]
        assert order_002["payment_types_nunique"] == 2

    def test_review_is_present(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        orders = gold_tables["gold_fct_orders"]
        order_001 = orders[orders["order_id"] == "order_001"].iloc[0]
        assert order_001["review_score_mean"] == 5.0

    def test_review_is_missing_for_order_without_feedback(
        self, gold_tables: dict[str, pd.DataFrame]
    ) -> None:
        orders = gold_tables["gold_fct_orders"]
        order_003 = orders[orders["order_id"] == "order_003"].iloc[0]
        assert pd.isna(order_003["review_score_mean"])

    def test_purchase_year_month_format(
        self, gold_tables: dict[str, pd.DataFrame]
    ) -> None:
        orders = gold_tables["gold_fct_orders"]
        for value in orders["purchase_year_month"].dropna():
            assert len(value) == 7
            assert value[4] == "-"


class TestGoldAggSalesMonthly:
    def test_returns_monthly_aggregate(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        assert "gold_agg_sales_monthly" in gold_tables

    def test_otd_rate_is_between_zero_and_one(
        self, gold_tables: dict[str, pd.DataFrame]
    ) -> None:
        aggregate = gold_tables["gold_agg_sales_monthly"]
        rates = aggregate["otd_rate"].dropna()
        assert (rates >= 0).all()
        assert (rates <= 1).all()

    def test_otd_rate_excludes_open_orders(
        self, gold_tables: dict[str, pd.DataFrame]
    ) -> None:
        aggregate = gold_tables["gold_agg_sales_monthly"]
        january = aggregate[aggregate["purchase_year_month"] == "2022-01"].iloc[0]
        assert abs(january["otd_rate"] - (2 / 3)) < 0.01

    def test_revenue_total_is_positive(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        aggregate = gold_tables["gold_agg_sales_monthly"]
        assert (aggregate["revenue_total"].dropna() > 0).all()

    def test_orders_count_is_correct(self, gold_tables: dict[str, pd.DataFrame]) -> None:
        aggregate = gold_tables["gold_agg_sales_monthly"]
        january = aggregate[aggregate["purchase_year_month"] == "2022-01"].iloc[0]
        assert january["orders_count"] == 3


class TestGoldDimTables:
    def test_customers_dimension_has_geo_columns(
        self, gold_tables: dict[str, pd.DataFrame]
    ) -> None:
        customers = gold_tables["gold_dim_customers"]
        assert "customer_lat" in customers.columns
        assert "customer_lng" in customers.columns

    def test_products_dimension_has_english_category(
        self, gold_tables: dict[str, pd.DataFrame]
    ) -> None:
        products = gold_tables["gold_dim_products"]
        electronics = products[products["product_category_name"] == "eletronicos"]
        assert "product_category_name_english" in products.columns
        assert (electronics["product_category_name_english"] == "electronics").all()

    def test_sellers_dimension_has_geo_columns(
        self, gold_tables: dict[str, pd.DataFrame]
    ) -> None:
        sellers = gold_tables["gold_dim_sellers"]
        assert "seller_lat" in sellers.columns
        assert "seller_lng" in sellers.columns
