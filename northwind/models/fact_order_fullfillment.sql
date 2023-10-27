with stg_orders as(
    SELECT 
        orderid, 
        {{dbt_utils.generate_surrogate_key(['employeeid'])}} as employeekey,
        {{dbt_utils.generate_surrogate_key(['customerid'])}} as customerkey,
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey,
        replace(to_date(shippeddate)::varchar,'-','')::int as shippeddatekey,
        replace(to_date(requireddate)::varchar,'-','')::int as requireddatekey,
        shipname,
        shipaddress, 
        shipcity,
        shipregion,
        shippostalcode,
        shipcountry,
        freight,
        shipvia 
FROM {{ source('northwind', 'Orders') }}
),
stg_order_details as(
    SELECT 
        orderid, 
        sum(quantity) as quantityonorder, 
        sum(quantity*UnitPrice*(1-Discount)) as totalorderamount
    FROM {{ source('northwind', 'Order_Details') }}
    GROUP BY orderid
),
stg_shippers as (
    SELECT * FROM {{ source('northwind', 'Shippers') }}
)

SELECT
    o.*,
    s.companyname as shippercompanyname,
    od.quantityonorder,
    od.totalorderamount,
    o.shippeddatekey - o.orderdatekey as daysfromordertoshipped,
    o.requireddatekey - o.orderdatekey as daysfromordertorequired,
    o.shippeddatekey - o.requireddatekey as shippedtorequireddelta,
    CASE
        WHEN o.shippeddatekey - o.requireddatekey <=0 THEN 'Y' ELSE 'N'
    END as shippedontime
FROM stg_orders o
    JOIN stg_order_details od on o.orderid = od.orderid
    JOIN stg_shippers s on s.shipperid = o.shipvia