-- Gold transformations executed in in-memory SQLite after the Silver load.

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

CREATE TEMP TABLE gold_dim_customers_sql AS
SELECT
    customers.*,
    geolocations.latitude AS customer_lat,
    geolocations.longitude AS customer_lng,
    geolocations.city AS customer_geo_city,
    geolocations.state AS customer_geo_state
FROM customers
LEFT JOIN geolocations
    ON customers.customer_zip_code_prefix = geolocations.zip_code_prefix;

CREATE TEMP TABLE gold_dim_sellers_sql AS
SELECT
    sellers.*,
    geolocations.latitude AS seller_lat,
    geolocations.longitude AS seller_lng,
    geolocations.city AS seller_geo_city,
    geolocations.state AS seller_geo_state
FROM sellers
LEFT JOIN geolocations
    ON sellers.seller_zip_code_prefix = geolocations.zip_code_prefix;

CREATE TEMP TABLE gold_dim_products_sql AS
SELECT
    products.*,
    product_categories.product_category_name_english
FROM products
LEFT JOIN product_categories
    ON products.product_category_name = product_categories.product_category_name;

CREATE TEMP TABLE gold_fct_orders_sql AS
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

CREATE TEMP TABLE gold_fct_order_items_sql AS
SELECT
    order_items_prepared.*,
    gold_fct_orders_sql.customer_id,
    gold_fct_orders_sql.order_status,
    gold_fct_orders_sql.order_purchase_timestamp,
    gold_fct_orders_sql.order_delivered_carrier_date,
    gold_fct_orders_sql.order_delivered_customer_date,
    gold_fct_orders_sql.order_estimated_delivery_date,
    gold_fct_orders_sql.purchase_year,
    gold_fct_orders_sql.purchase_month,
    gold_fct_orders_sql.purchase_year_month,
    gold_fct_orders_sql.delivery_days,
    gold_fct_orders_sql.delivery_delay_days,
    gold_fct_orders_sql.is_delivered_late,
    julianday(order_items_prepared.shipping_limit_date) - julianday(gold_fct_orders_sql.order_delivered_carrier_date) AS shipping_accuracy_days
FROM order_items_prepared
LEFT JOIN gold_fct_orders_sql
    ON order_items_prepared.order_id = gold_fct_orders_sql.order_id;

CREATE TEMP TABLE gold_agg_sales_monthly_sql AS
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
FROM gold_fct_orders_sql
GROUP BY purchase_year, purchase_month, purchase_year_month;
