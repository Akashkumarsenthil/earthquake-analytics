select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select latitude
from USER_DB_PLATYPUS.ANALYTICS.fct_earthquakes
where latitude is null



      
    ) dbt_internal_test