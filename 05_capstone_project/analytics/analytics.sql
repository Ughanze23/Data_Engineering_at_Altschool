--Which product categories have the highest sales?
select product_category,total_sales
from `alt-school-423017.ecommerce_prod_final.fct_sales_by_category`
order by 2 desc
Limit 10;

--What is the average delivery time for orders?
select ROUND(AVG(days_diff)) as avg_delivery_time_in_days, ROUND(AVG(hours_diff),2) as avg_delivery_time_hours
from `alt-school-423017.ecommerce_prod_final.fct_delivery_time`;

--Which states have the highest number of orders?
select state, sum(order_count) as total_orders
from `alt-school-423017.ecommerce_prod_final.fct_orders_by_state` o
JOIN (select distinct zip_code,state from `alt-school-423017.ecommerce_prod_final.dim_location`)  as l ON l.zip_code = o.customer_zip_code
group by 1
order by 2 DESC
limit 10;
