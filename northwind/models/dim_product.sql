with stg_products as (
    SELECT * FROM {{ source('northwind', 'Products') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['stg_products.productid'])}} as productkey,
    stg_products.*
FROM stg_products