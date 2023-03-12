with payments as (
    select order_id
    ,sum(amount) as amount
    from {{ ref('stg_payment') }}
    where status = 'success'
    group by order_id
)
select ord.order_id,
customer.customer_id,
pay.amount
from {{ ref('stg_orders') }} as ord
left join {{ ref('stg_customers') }} as customer
on ord.customer_id = customer.customer_id
left join payments as pay
on ord.order_id = pay.order_id