create or replace view nest_daily_summary_with_weather as 

with nest as 

(

select * 
from nest_daily_summary nds 

),

py_weather as

(

select date(dt) as date, avg(temp) as mke_temp_py, avg(feels_like) as mke_feels_like_py
from milwaukee_modeled mm 
group by 1
order by 1

),

cy_weather as

(

select date(dt) as date, avg(temp) as mke_temp_cy, avg(feels_like) as mke_feels_like_cy
from milwaukee_modeled mm 
group by 1
order by 1

)

select nest.*,
	   round(py_weather.mke_temp_py,2) as mke_temp_py,
	   round(py_weather.mke_feels_like_py,2) as mke_feels_like_py,
	   round(cy_weather.mke_temp_cy,2) as mke_temp_cy,
	   round(cy_weather.mke_feels_like_cy,2) as mke_feels_like_cy
from nest

left join py_weather
on (date(nest.py_date) = date(py_weather.date))

left join cy_weather
on (date(nest.cy_date) = date(cy_weather.date))


