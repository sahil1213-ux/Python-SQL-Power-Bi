create or replace database bike_database;
use bike_database;

create or replace view joined_data as 
with cte as (
select *
from fact_bike)

select 
       to_date(c.dteday, 'DD/MM/YYYY') as date,
       c.season,
       c.yr as year,
       c.weekday,
       c.hr as hour,
       c.rider_type,
       c.riders,
       dc.price, 
       dc.cogs,
       c.riders * dc.price as revenue,
       (c.riders * dc.price) - (c.riders * dc.cogs) AS profit
from cte c
left join dim_cost dc on c.yr = dc.yr
order by extract(day from date) asc, extract(month from date) asc, year asc;


select * from joined_data;

select rider_type, count(rider_type) as rider_counts
from joined_data
group by 1;




-- Get the KPI's: Total Riders, Total Revenue, Total Profit

select to_varchar(sum(riders), '999,999,999') as total_riders, 
       to_varchar(sum(revenue), '999,999,999') as total_revenue, 
       to_varchar(sum(profit), '999,999,999') as total_profit
from joined_data;



-- Average Rider Value (revenue)

create or replace view average_rider_value as
select rider_type, round(avg(revenue), 2) as average_revenue
from joined_data
group by 1
order by 2 desc;

select * from average_rider_value;


-- Average Rider Value in Per year (revenue)

create or replace view average_rider_value_per_year as
select extract(year from date) as years,
       round(avg(case when rider_type = 'registered' then revenue else null end), 2) as avg_rev_registered,
       round(avg(case when rider_type = 'casual' then revenue else null end), 2) as avg_rev_casual
from joined_data
group by 1;


select * from average_rider_value_per_year;


-- Rider Demographies 

create or replace view average_rider_per_year as
select extract(year from date) as year,
       round(avg(riders), 2) as average_riders
from joined_data
group by 1
order by 2 desc;

select * from BIKE_DATABASE.PUBLIC.AVERAGE_RIDER_PER_YEAR;



create or replace view riders_type_count as
select rider_type, count(rider_type) as total_riders
from joined_data
group by 1;

select * from riders_type_count;




-- Hourly Revenue Analysis

create or replace view hourly_revenue as
select hour, round(avg(revenue), 2) as average_revenue
from joined_data
group by 1
order by 2 desc;

select * from hourly_revenue;


-- Profit and Revenue Trends

create or replace view profit_revenue_trend as
select date_trunc('month', date) as start_of_month,
       round(avg(revenue), 2) as average_revenue,
       round(avg(profit), 2) as average_profit,
       sum(riders) as total_riders
from joined_data
group by 1
order by 1 asc;

select * from profit_revenue_trend;



-- MoM Analysis

create or replace view mom_analysis as
select 
    date_trunc('month', date) as month,
    sum(revenue) as total_revenue,
    sum(profit) as total_profit,
    sum(riders) as total_riders,
    
    lag(sum(revenue)) over (order by date_trunc('month', date)) as prev_month_revenue,
    -- mom change in revenue (%)
    ((sum(revenue) - lag(sum(revenue)) over (order by date_trunc('month', date))) / 
      nullif(lag(sum(revenue)) over (order by date_trunc('month', date)), 0)) * 100 as mom_revenue_change,
    
    lag(sum(profit)) over (order by date_trunc('month', date)) as prev_month_profit,
    -- mom change in profit (%)
    ((sum(profit) - lag(sum(profit)) over (order by date_trunc('month', date))) / 
      nullif(lag(sum(profit)) over (order by date_trunc('month', date)), 0)) * 100 as mom_profit_change,
      
    lag(sum(riders)) over (order by date_trunc('month', date)) as prev_month_riders,
    -- mom change in riders (%)
    ((sum(riders) - lag(sum(riders)) over (order by date_trunc('month', date))) / 
      nullif(lag(sum(riders)) over (order by date_trunc('month', date)), 0)) * 100 as mom_riders_change
      
from joined_data
group by date_trunc('month', date)
order by month;

select * from mom_analysis;




-- Identifying Peak Revenue Days by Season and Year

create or replace view peak_revenue_by_season as
with cte1 as (
select date,
       season, 
       round(avg(revenue), 2) as average_revenue
from joined_data
group by 1, 2
),

cte2 as (
select *,
       row_number() over(partition by extract(year from date), season order by average_revenue desc) as rn
from cte1
)

select extract(year from date) as year,
       season,
       average_revenue
from cte2
where rn = 1;

select * from peak_revenue_by_season;