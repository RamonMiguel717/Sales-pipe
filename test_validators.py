import pandas as pd
import pytest

from core.validators import (
    validate_no_duplicates,
    validate_no_nulls,
    validate_row_count,
    validate_value_range,
)


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["a", "b", "c"],
            "value": [1.0, 2.0, 3.0],
            "name": ["x", None, "z"],
        }
    )


class TestValidateNoNulls:
    def test_without_nulls_returns_true(self, sample_df: pd.DataFrame) -> None:
        assert validate_no_nulls(sample_df, ["id", "value"], "test") is True

    def test_with_nulls_returns_false(self, sample_df: pd.DataFrame) -> None:
        assert validate_no_nulls(sample_df, ["name"], "test") is False

    def test_raise_on_error_raises_exception(self, sample_df: pd.DataFrame) -> None:
        with pytest.raises(ValueError):
            validate_no_nulls(sample_df, ["name"], "test", raise_on_error=True)

    def test_missing_column_returns_false(self, sample_df: pd.DataFrame) -> None:
        assert validate_no_nulls(sample_df, ["missing"], "test") is False


class TestValidateRowCount:
    def test_above_minimum_returns_true(self, sample_df: pd.DataFrame) -> None:
        assert validate_row_count(sample_df, min_rows=2, table_name="test") is True

    def test_below_minimum_raises_exception(self, sample_df: pd.DataFrame) -> None:
        with pytest.raises(ValueError):
            validate_row_count(sample_df, min_rows=10, table_name="test")

    def test_below_minimum_without_raise_returns_false(
        self, sample_df: pd.DataFrame
    ) -> None:
        assert validate_row_count(
            sample_df,
            min_rows=10,
            table_name="test",
            raise_on_error=False,
        ) is False

    def test_exactly_at_minimum_returns_true(self, sample_df: pd.DataFrame) -> None:
        assert validate_row_count(sample_df, min_rows=3, table_name="test") is True


class TestValidateNoDuplicates:
    def test_without_duplicates_returns_true(self, sample_df: pd.DataFrame) -> None:
        assert validate_no_duplicates(sample_df, ["id"], "test") is True

    def test_with_duplicates_returns_false(self) -> None:
        dataframe = pd.DataFrame({"id": ["a", "a", "b"], "value": [1, 2, 3]})
        assert validate_no_duplicates(dataframe, ["id"], "test") is False

    def test_raise_on_error_raises_exception(self) -> None:
        dataframe = pd.DataFrame({"id": ["a", "a", "b"]})
        with pytest.raises(ValueError):
            validate_no_duplicates(dataframe, ["id"], "test", raise_on_error=True)


class TestValidateValueRange:
    def test_values_within_range_return_true(self, sample_df: pd.DataFrame) -> None:
        assert validate_value_range(sample_df, "value", 0, 10, "test") is True

    def test_value_below_minimum_returns_false(self) -> None:
        dataframe = pd.DataFrame({"value": [-1.0, 2.0, 3.0]})
        assert validate_value_range(dataframe, "value", 0, None, "test") is False

    def test_value_above_maximum_returns_false(self) -> None:
        dataframe = pd.DataFrame({"value": [0.5, 1.5, 2.0]})
        assert validate_value_range(dataframe, "value", 0, 1, "test") is False

    def test_missing_column_returns_false(self, sample_df: pd.DataFrame) -> None:
        assert validate_value_range(sample_df, "missing", 0, 10, "test") is False

    def test_no_upper_limit_accepts_any_positive_value(self) -> None:
        dataframe = pd.DataFrame({"value": [1.0, 999.0, 100000.0]})
        assert validate_value_range(dataframe, "value", 0, None, "test") is True
