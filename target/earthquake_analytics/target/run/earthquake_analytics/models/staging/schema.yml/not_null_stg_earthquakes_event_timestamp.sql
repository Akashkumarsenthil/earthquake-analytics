select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select event_timestamp
from USER_DB_PLATYPUS.ANALYTICS.stg_earthquakes
where event_timestamp is null



      
    ) dbt_internal_test