WITH stg_orders as(
    SELECT  
        orderid, 
        {{ dbt_utils.generate_surrogate_key(['customerid'])}} as customerkey,
        {{ dbt_utils.generate_surrogate_key(['employeeid'])}} as employeekey,
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey
    FROM {{ source('northwind', 'Orders') }}
),
stg_order_details as(
    SELECT
        orderid, productid,
        sum(quantity) as quantity,
        sum(quantity * UnitPrice) as extendedpriceamount, 
        sum(quantity * UnitPrice * Discount) as discountamount
    FROM {{ source('northwind', 'Order_Details') }}
    GROUP BY orderid, Discount, productid
), 
stg_products as (
    SELECT 
        productid,
        {{ dbt_utils.generate_surrogate_key(['productid'])}} as productkey
    FROM {{ source('northwind', 'Products') }}
)


SELECT 
    o.orderid, 
    o.customerkey,
    o.employeekey,
    o.orderdatekey,
    p.productkey,
    od.quantity,
    od.extendedpriceamount,
    od.discountamount,
    (od.extendedpriceamount - od.discountamount) as soldamount
FROM stg_orders o 
JOIN stg_order_details od on o.orderid = od.orderid
JOIN stg_products p on od.productid = p.productid

