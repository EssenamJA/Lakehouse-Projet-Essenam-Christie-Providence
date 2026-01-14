
    
    

with all_values as (

    select
        http_method as value_field,
        count(*) as n_records

    from "wintershop_student"."wintershop_essenam"."silver"
    group by http_method

)

select *
from all_values
where value_field not in (
    'GET','POST','PUT','DELETE','HEAD','OPTIONS'
)


