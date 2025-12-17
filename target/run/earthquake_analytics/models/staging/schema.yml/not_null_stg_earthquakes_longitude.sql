select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select longitude
from USER_DB_PLATYPUS.ANALYTICS.stg_earthquakes
where longitude is null



      
    ) dbt_internal_test