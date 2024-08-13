
    SELECT 
    p.product_id,
    p.category_id,
     cat.product_category,
     p.product_name_length,
         p.product_description_length,
         p.product_photos_qty ,
         p.product_weight_g ,
         p.product_length_cm ,
         p.product_height_cm ,
         p.product_width_cm  
    FROM {{ref("stg_products")}} AS p
    JOIN {{ref("stg_product_category")}} AS cat ON p.category_id = cat.category_id



