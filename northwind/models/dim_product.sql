with stg_products as (
    SELECT * FROM {{ source('northwind', 'Products') }}
),
stg_categories as (
    SELECT * FROM {{ source('northwind', 'Categories') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['s.productid'])}} as productkey,
    s.productid as productid,
    s.productname,
    s.supplierid as supplierkey,
    c.categoryname,
    c.description as categorydescription

FROM stg_products s 
LEFT JOIN stg_categories c on s.categoryid = c.categoryid