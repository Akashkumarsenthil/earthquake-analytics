select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select magnitude
from USER_DB_PLATYPUS.ANALYTICS.stg_earthquakes
where magnitude is null



      
    ) dbt_internal_test