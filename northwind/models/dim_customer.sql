with stg_customers as (
    SELECT * FROM {{ source('northwind', 'Customers') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['stg_customers.customerid'])}} as customerkey,
    stg_customers.*
FROM stg_customers