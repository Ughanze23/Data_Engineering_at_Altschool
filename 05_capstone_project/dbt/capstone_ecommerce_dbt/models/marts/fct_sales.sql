WITH sales AS (
    SELECT 
        order_id,
        product_id, 
        seller_id, 
        price,
        freight_value,
        shipping_limit_date,
        MAX(order_item_id) AS no_of_items
    FROM  {{ref("stg_order_items")}} 
    GROUP BY order_id,product_id, seller_id, price,freight_value,shipping_limit_date)

    ,

    total_sales AS (
        SELECT 
        order_id,
        product_id, 
        seller_id, 
        price,
        freight_value,
        shipping_limit_date,
        (no_of_items * price) as total_price,
        (freight_value * price) as total_freight_value
        FROM sales
    )
        ,
    total_order_value as (
        SELECT 
        order_id,
        product_id, 
        seller_id, 
        price,
        freight_value,
        shipping_limit_date,
        total_price,
        total_freight_value,
        (total_price + total_freight_value) AS total_order_value
        FROM total_sales
    )

    SELECT * FROM total_order_value