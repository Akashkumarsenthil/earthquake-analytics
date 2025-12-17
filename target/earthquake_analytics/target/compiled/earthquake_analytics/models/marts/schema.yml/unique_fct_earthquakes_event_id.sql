
    
    

select
    event_id as unique_field,
    count(*) as n_records

from USER_DB_PLATYPUS.ANALYTICS.fct_earthquakes
where event_id is not null
group by event_id
having count(*) > 1


