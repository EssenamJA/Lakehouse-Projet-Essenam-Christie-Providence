
    
    

with all_values as (

    select
        status_code as value_field,
        count(*) as n_records

    from "wintershop_student"."wintershop_essenam"."silver"
    group by status_code

)

select *
from all_values
where value_field not in (
    '200','301','302','400','401','403','404','500','503'
)


