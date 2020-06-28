########### work arond for looping in big query 
#### use public data set 
-- creating table , create in your own user dataset , the same is used later too while inserting 
create table Sample.Distinct_country (
SINO int64,
country string 
);
############  block begins 
begin
--- declaring var for loop 
declare lop int64 default 1;
declare cnt int64 default 0;
-- getting data for loop
create temp table tmp_country as
select country, rank() over(order by country) rnk 
from (select distinct country
from `bigquery-public-data.faa.us_airports`)
order by country limit 5 ;
--- set how many loops 
set cnt= (select count(*) from tmp_country);
--- loop starts 
while lop<=cnt do
insert into Sample.Distinct_country 
  select lop, country 
  from tmp_country
  where rnk = lop;
--- increase loop 
set lop=lop+1;

end while;
end;
################
--- query the inserted data
select * from Sample.Distinct_country limit 5;

