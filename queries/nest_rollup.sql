create or replace view nest_rollup as 

select max(py_date) as py_date,
	   sum(py_obs) as py_obs,
	   sum(py_heat) as py_heat,
	   avg(py_average_temperature) as py_average_temperature,
	   max(cy_date) as cy_date,
	   sum(cy_obs) as cy_obs,
	   sum(cy_heat) as cy_heat,
	   avg(cy_average_temperature) as cy_average_temperature,
	   avg(mke_temp_py) as mke_temp_py,
	   avg(mke_feels_like_py) as mke_feels_like_py,
	   avg(mke_temp_cy) as mke_temp_cy,
	   avg(mke_feels_like_cy) as mke_feels_like_cy
from nest_daily_summary_with_weather;