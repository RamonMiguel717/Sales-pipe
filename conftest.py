import pandas as pd
import pytest


@pytest.fixture
def raw_orders() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": ["order_001", "order_002", "order_003", "order_004"],
            "customer_id": ["cust_001", "cust_002", "cust_003", "cust_004"],
            "order_status": ["delivered", "delivered", "shipped", "delivered"],
            "order_purchase_timestamp": pd.to_datetime(
                [
                    "2022-01-01 10:00:00",
                    "2022-01-05 14:00:00",
                    "2022-02-01 09:00:00",
                    "2022-01-15 11:00:00",
                ]
            ),
            "order_approved_at": pd.to_datetime(
                [
                    "2022-01-01 10:30:00",
                    "2022-01-05 14:45:00",
                    "2022-02-01 09:30:00",
                    "2022-01-15 11:15:00",
                ]
            ),
            "order_delivered_carrier_date": pd.to_datetime(
                [
                    "2022-01-03 08:00:00",
                    "2022-01-07 12:00:00",
                    None,
                    "2022-01-17 10:00:00",
                ]
            ),
            "order_delivered_customer_date": pd.to_datetime(
                [
                    "2022-01-10 15:00:00",
                    "2022-01-25 10:00:00",
                    None,
                    "2022-01-22 14:00:00",
                ]
            ),
            "order_estimated_delivery_date": pd.to_datetime(
                [
                    "2022-01-15 00:00:00",
                    "2022-01-20 00:00:00",
                    "2022-02-20 00:00:00",
                    "2022-01-25 00:00:00",
                ]
            ),
        }
    )


@pytest.fixture
def raw_order_items() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": ["order_001", "order_001", "order_002", "order_003", "order_004"],
            "order_item_id": [1, 2, 1, 1, 1],
            "product_id": ["prod_A", "prod_B", "prod_C", "prod_D", "prod_A"],
            "seller_id": ["seller_1", "seller_2", "seller_1", "seller_3", "seller_2"],
            "shipping_limit_date": pd.to_datetime(
                ["2022-01-04", "2022-01-04", "2022-01-08", "2022-02-05", "2022-01-18"]
            ),
            "price": [100.0, 50.0, 200.0, 80.0, 120.0],
            "freight_value": [10.0, 5.0, 20.0, 15.0, 12.0],
        }
    )


@pytest.fixture
def raw_order_payments() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": ["order_001", "order_002", "order_002", "order_003", "order_004"],
            "payment_sequential": [1, 1, 2, 1, 1],
            "payment_type": ["credit_card", "boleto", "voucher", "credit_card", "debit_card"],
            "payment_installments": [3, 1, 1, 2, 1],
            "payment_value": [115.0, 180.0, 40.0, 95.0, 132.0],
        }
    )


@pytest.fixture
def raw_order_reviews() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "review_id": ["rev_001", "rev_002", "rev_004"],
            "order_id": ["order_001", "order_002", "order_004"],
            "review_score": [5, 2, 4],
            "review_comment_title": ["Otimo!", None, None],
            "review_comment_message": ["Chegou rapido", "Atrasou muito", None],
            "review_creation_date": pd.to_datetime(["2022-01-11", "2022-01-26", "2022-01-23"]),
            "review_answer_timestamp": pd.to_datetime(
                ["2022-01-12", "2022-01-27", "2022-01-24"]
            ),
        }
    )


@pytest.fixture
def raw_customers() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "customer_id": ["cust_001", "cust_002", "cust_003", "cust_004"],
            "customer_unique_id": ["uniq_001", "uniq_002", "uniq_003", "uniq_004"],
            "customer_zip_code_prefix": [1310, 4520, 6700, 9000],
            "customer_city": ["Sao Paulo", "Sao Paulo", "Rio de Janeiro", "Porto Alegre"],
            "customer_state": ["SP", "SP", "RJ", "RS"],
        }
    )


@pytest.fixture
def raw_sellers() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "seller_id": ["seller_1", "seller_2", "seller_3"],
            "seller_zip_code_prefix": [1310, 4520, 6700],
            "seller_city": ["Sao Paulo", "Sao Paulo", "Rio de Janeiro"],
            "seller_state": ["SP", "SP", "RJ"],
        }
    )


@pytest.fixture
def raw_products() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_id": ["prod_A", "prod_B", "prod_C", "prod_D"],
            "product_category_name": ["eletronicos", "livros", "eletronicos", "esporte"],
            "product_name_lenght": [20, 15, 18, 12],
            "product_description_lenght": [100, 80, 90, 60],
            "product_photos_qty": [3, 1, 2, 2],
            "product_weight_g": [500, 200, 800, 300],
            "product_length_cm": [30, 20, 35, 25],
            "product_height_cm": [10, 5, 12, 8],
            "product_width_cm": [20, 15, 25, 18],
        }
    )


@pytest.fixture
def raw_translation() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_category_name": ["eletronicos", "livros", "esporte"],
            "product_category_name_english": ["electronics", "books", "sports"],
        }
    )


@pytest.fixture
def raw_geolocation() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "geolocation_zip_code_prefix": [1310, 1310, 4520, 6700, 9000],
            "geolocation_lat": [-23.5, -23.52, -23.6, -22.9, -30.0],
            "geolocation_lng": [-46.6, -46.62, -46.7, -43.2, -51.2],
            "geolocation_city": [
                "sao paulo",
                "sao paulo",
                "sao paulo",
                "rio de janeiro",
                "porto alegre",
            ],
            "geolocation_state": ["SP", "SP", "SP", "RJ", "RS"],
        }
    )


@pytest.fixture
def raw_tables(
    raw_orders: pd.DataFrame,
    raw_order_items: pd.DataFrame,
    raw_order_payments: pd.DataFrame,
    raw_order_reviews: pd.DataFrame,
    raw_customers: pd.DataFrame,
    raw_sellers: pd.DataFrame,
    raw_products: pd.DataFrame,
    raw_translation: pd.DataFrame,
    raw_geolocation: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    return {
        "orders": raw_orders,
        "order_items": raw_order_items,
        "order_payments": raw_order_payments,
        "order_reviews": raw_order_reviews,
        "customers": raw_customers,
        "sellers": raw_sellers,
        "products": raw_products,
        "translation": raw_translation,
        "geolocation": raw_geolocation,
    }
