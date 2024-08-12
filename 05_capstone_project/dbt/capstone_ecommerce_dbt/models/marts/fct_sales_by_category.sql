SELECT product_category,sum(total_price) as total_sales FROM {{ref("int_sales_by_category")}}
GROUP BY product_category
ORDER BY 2 DESC