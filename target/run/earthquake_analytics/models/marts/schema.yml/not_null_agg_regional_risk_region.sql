select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select region
from USER_DB_PLATYPUS.ANALYTICS.agg_regional_risk
where region is null



      
    ) dbt_internal_test