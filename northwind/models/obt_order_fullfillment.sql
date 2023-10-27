with f_order_fullfillment as (
    SELECT * FROM {{ ref('fact_order_fullfillment') }}
),
d_customer as (
    SELECT * FROM {{ ref('dim_customer') }}
),
d_employee as (
    SELECT * FROM {{ ref('dim_employee') }}
),
d_date as (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT 
    d_customer.*,
    d_employee.*,
    d_date.*,
    f.orderid, 
    f.orderdatekey, 
    f.shippeddatekey, 
    f.requireddatekey, 
    f.shipname, 
    f.shipaddress, 
    f.shipcity, 
    f.shipregion, 
    f.shippostalcode, 
    f.shipcountry,
    f.freight, 
    f.shipvia, 
    f.shippercompanyname, 
    f.quantityonorder, 
    f.totalorderamount, 
    f.daysfromordertoshipped, 
    f.daysfromordertorequired, 
    f.shippedtorequireddelta, 
    f.shippedontime
FROM f_order_fullfillment as f
    LEFT JOIN d_customer on f.customerkey = d_customer.customerkey
    LEFT JOIN d_employee on f.employeekey = d_employee.employeekey
    LEFT JOIN d_date on f.orderdatekey = d_date.datekey
