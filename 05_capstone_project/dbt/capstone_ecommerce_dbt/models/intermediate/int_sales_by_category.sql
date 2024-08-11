WITH sales AS (
    SELECT 
        o.order_id,
        oi.product_id, 
        cat.product_category, 
        oi.price,
        MAX(order_item_id) AS no_of_items
    FROM 
        {{ref("stg_orders")}} AS o
    LEFT JOIN 
        {{ref("stg_order_items")}} AS oi ON oi.order_id = o.order_id 
    LEFT JOIN 
        {{ref("stg_products")}} AS p ON p.product_id = oi.product_id
    LEFT JOIN 
        {{ref("stg_product_category")}} AS cat ON cat.category_id = p.category_id
    
    WHERE order_status in ("delivered","invoiced","shipped","approved")
    GROUP BY 
        o.order_id, oi.product_id, cat.product_category, oi.price 

),

sales_by_category AS (
    SELECT 
        product_id,
        product_category, 
        (price * no_of_items) AS total_sales
    FROM 
        sales
)

SELECT * 
FROM sales_by_category
