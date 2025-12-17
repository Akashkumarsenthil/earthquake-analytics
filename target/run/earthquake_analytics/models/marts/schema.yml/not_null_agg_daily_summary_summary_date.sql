select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select summary_date
from USER_DB_PLATYPUS.ANALYTICS.agg_daily_summary
where summary_date is null



      
    ) dbt_internal_test