	select 
	
		cast(cast(replace(orderdate,'-','') as varchar(10))as int) as datekey,
		orderdate,
		weekday_start,
		newxtweek_startat,
		nextweek_fromdate,
		firstmondayinmonth,
		lastmondayinmonth,
		orderyear,
		monthname,
		monthabrv,
		monthnum,
		quartermonth,
		yearmonth,
		yearmonthnum,
		lastdayofmonth,
		weekname,
		weeknameabrv,
		isweekday,
		daynum,
		isweekbegin,
		ismonthbegin,
		islastdayofmonth,
		leapyear

	from {{ ref ('dimdate') }}