Practice
Complete the exercise in this section in your dbt project. Remember to make adjustments for the data platform you are using and refer to Quickstarts and docs if you encounter issues. Debug issues and consult the documentation for dbt and your data platform as part of the practice exercise. REMINDER: BigQuery users, your database name is not raw, it is dbt-tutorial. 

Quick Project Polishing
In your dbt_project.yml file, change the name of your project (at the top of the project AND underneath the 'models' heading further down)
Staging Models
Create a staging/jaffle_shop directory in your models folder.
Create a stg_jaffle_shop__customers.sql model for raw.jaffle_shop.customers
select
    id as customer_id,
    first_name,
    last_name

from raw.jaffle_shop.customers
Create a stg_jaffle_shop__orders.sql model for raw.jaffle_shop.orders
select
    id as order_id,
    user_id as customer_id,
    order_date,
    status

from raw.jaffle_shop.orders
Mart Models
Create a marts/marketing directory in your models folder.
Create a dim_customers.sql model
with customers as (

     select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as ( 

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce (customer_orders.number_of_orders, 0) 
        as number_of_orders

    from customers

    left join customer_orders using (customer_id)

)

select * from final