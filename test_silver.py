import pandas as pd

from pipe.sales import _mode_or_first, _prepare_silver_tables


class TestModeOrFirst:
    def test_returns_mode_for_regular_series(self) -> None:
        series = pd.Series(["SP", "SP", "RJ", "SP"])
        assert _mode_or_first(series) == "SP"

    def test_returns_only_value(self) -> None:
        series = pd.Series(["RJ"])
        assert _mode_or_first(series) == "RJ"

    def test_returns_na_for_empty_series(self) -> None:
        result = _mode_or_first(pd.Series([], dtype=str))
        assert pd.isna(result)

    def test_ignores_nulls_before_mode(self) -> None:
        series = pd.Series([None, "SP", "SP", None])
        assert _mode_or_first(series) == "SP"

    def test_all_null_series_returns_na(self) -> None:
        series = pd.Series([None, None, None])
        assert pd.isna(_mode_or_first(series))


class TestPrepareSilverTables:
    def test_returns_expected_tables(self, raw_tables: dict[str, pd.DataFrame]) -> None:
        silver = _prepare_silver_tables(raw_tables)
        expected = {
            "silver_geolocations",
            "silver_product_categories",
            "silver_customers",
            "silver_sellers",
            "silver_products",
            "silver_orders",
            "silver_order_items",
            "silver_order_payments",
            "silver_order_reviews",
        }
        assert set(silver.keys()) == expected

    def test_geolocation_is_aggregated_by_zip(self, raw_tables: dict[str, pd.DataFrame]) -> None:
        silver = _prepare_silver_tables(raw_tables)
        geolocations = silver["silver_geolocations"]
        zip_1310 = geolocations[geolocations["zip_code_prefix"] == 1310]
        assert len(zip_1310) == 1

    def test_geolocation_latitude_uses_mean(self, raw_tables: dict[str, pd.DataFrame]) -> None:
        silver = _prepare_silver_tables(raw_tables)
        geolocations = silver["silver_geolocations"]
        zip_1310 = geolocations[geolocations["zip_code_prefix"] == 1310].iloc[0]
        assert abs(zip_1310["latitude"] - (-23.51)) < 0.001

    def test_geolocation_city_uses_mode(self, raw_tables: dict[str, pd.DataFrame]) -> None:
        silver = _prepare_silver_tables(raw_tables)
        geolocations = silver["silver_geolocations"]
        zip_1310 = geolocations[geolocations["zip_code_prefix"] == 1310].iloc[0]
        assert zip_1310["city"] == "sao paulo"

    def test_missing_zip_prefixes_are_added_to_geolocations(
        self,
        raw_tables: dict[str, pd.DataFrame],
    ) -> None:
        raw_tables["customers"].loc[0, "customer_zip_code_prefix"] = 99999
        raw_tables["customers"].loc[0, "customer_city"] = "cidade cliente"
        raw_tables["customers"].loc[0, "customer_state"] = "SP"
        raw_tables["sellers"].loc[0, "seller_zip_code_prefix"] = 88888
        raw_tables["sellers"].loc[0, "seller_city"] = "cidade seller"
        raw_tables["sellers"].loc[0, "seller_state"] = "RJ"

        silver = _prepare_silver_tables(raw_tables)
        geolocations = silver["silver_geolocations"].set_index("zip_code_prefix")

        assert 99999 in geolocations.index
        assert 88888 in geolocations.index
        assert geolocations.loc[99999, "city"] == "cidade cliente"
        assert geolocations.loc[88888, "state"] == "RJ"
        assert pd.isna(geolocations.loc[99999, "latitude"])
        assert pd.isna(geolocations.loc[88888, "longitude"])

    def test_missing_product_categories_are_added_to_translation(
        self,
        raw_tables: dict[str, pd.DataFrame],
    ) -> None:
        raw_tables["products"].loc[0, "product_category_name"] = "categoria_sem_traducao"

        silver = _prepare_silver_tables(raw_tables)
        categories = silver["silver_product_categories"].set_index(
            "product_category_name"
        )

        assert "categoria_sem_traducao" in categories.index
        assert pd.isna(
            categories.loc[
                "categoria_sem_traducao",
                "product_category_name_english",
            ]
        )

    def test_monetary_values_are_rounded(self, raw_tables: dict[str, pd.DataFrame]) -> None:
        silver = _prepare_silver_tables(raw_tables)
        order_items = silver["silver_order_items"]
        order_payments = silver["silver_order_payments"]

        for column in ["price", "freight_value"]:
            assert (order_items[column].round(2) == order_items[column]).all()

        assert (order_payments["payment_value"].round(2) == order_payments["payment_value"]).all()

    def test_orders_keep_original_row_count(self, raw_tables: dict[str, pd.DataFrame]) -> None:
        silver = _prepare_silver_tables(raw_tables)
        assert len(silver["silver_orders"]) == len(raw_tables["orders"])

    def test_translation_table_is_preserved(self, raw_tables: dict[str, pd.DataFrame]) -> None:
        silver = _prepare_silver_tables(raw_tables)
        assert len(silver["silver_product_categories"]) == len(raw_tables["translation"])
