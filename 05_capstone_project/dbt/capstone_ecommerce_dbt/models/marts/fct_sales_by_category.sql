SELECT product_category,ROUND(sum(total_price), 2) as total_sales FROM {{ref("int_sales_by_category")}}
GROUP BY product_category
ORDER BY 2 DESC