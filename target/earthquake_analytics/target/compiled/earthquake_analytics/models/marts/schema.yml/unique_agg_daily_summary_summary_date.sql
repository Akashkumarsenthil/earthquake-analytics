
    
    

select
    summary_date as unique_field,
    count(*) as n_records

from USER_DB_PLATYPUS.ANALYTICS.agg_daily_summary
where summary_date is not null
group by summary_date
having count(*) > 1


