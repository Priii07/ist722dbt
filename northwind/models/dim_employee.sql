WITH stg_employees as (
    SELECT * FROM {{ source('northwind', 'Employees') }}
), 
stg_supervisors as (
    SELECT * FROM {{ source('northwind', 'Employees') }}
)
SELECT 
    {{dbt_utils.generate_surrogate_key(['e.employeeid'])}} as employeekey,
    e.employeeid,
    concat(e.lastname , ', ' , e.firstname) as employeenamelastfirst,
    concat(e.firstname , ' ' , e.lastname) as employeenamefirstlast,
    e.title as employeetitle,
    concat(s.lastname, ', ' , s.firstname) as supervisornamelastfirst,
    concat(s.firstname , ' ' , s.lastname) as supervisornamefirstlast
FROM stg_employees e
LEFT JOIN stg_supervisors s on e.employeeid = s.employeeid