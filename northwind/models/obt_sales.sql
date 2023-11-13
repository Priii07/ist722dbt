with f_fact_sales as (
    SELECT * FROM {{ ref('fact_sales') }}
),
d_customer as (
    SELECT * FROM {{ ref('dim_customer') }}
),
d_employee as (
    SELECT * FROM {{ ref('dim_employee') }}
),
d_date as (
    SELECT * FROM {{ ref('dim_date') }}
),
d_product as (
    SELECT * FROM {{ ref('dim_product') }}
)

SELECT 
    d_customer.*,
    d_employee.*,
    d_date.*,
    d_product.*,
    f.quantity,
    f.extendedpriceamount,
    f.discountamount,
    f.soldamount
FROM f_fact_sales f
    LEFT JOIN d_employee on f.employeekey = d_employee.employeekey
    LEFT JOIN d_customer on f.customerkey = d_customer.customerkey
    LEFT JOIN d_date on f.orderdatekey = d_date.datekey
    LEFT JOIN d_product on f.productkey = d_product.productkey