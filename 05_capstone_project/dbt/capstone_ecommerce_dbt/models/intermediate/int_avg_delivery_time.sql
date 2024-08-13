with delivered_orders as (

    SELECT 
    o.order_id,
    product_id,
    seller_id,
    order_purchase_timestamp,
    order_delivered_customer_date
    FROM {{ref("stg_orders")}} AS o
    JOIN {{ref("stg_order_items")}} AS oi ON oi.order_id = o.order_id
    WHERE order_status = "delivered"

)
,
diff_btw_purchase_delivery AS (
    SELECT 
    order_id,
    product_id,
    seller_id,
    TIMESTAMP_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY) AS days_diff,
    TIMESTAMP_DIFF(order_delivered_customer_date, order_purchase_timestamp, HOUR) AS hours_diff
    FROM delivered_orders
)

SELECT *  FROM diff_btw_purchase_delivery