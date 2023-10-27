WITH stg_employees as (
    SELECT * FROM {{ source('northwind', 'Employees') }}
), 
stg_supervisors as (
    SELECT * FROM {{ source('northwind', 'Employees') }}
)
SELECT * FROM stg_employees
