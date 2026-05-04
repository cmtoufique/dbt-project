select 
    customer_id
    ,order_id
    ,total_amount
from {{ ref('stg_jaffle_shop_orders') }}
join stg_strip