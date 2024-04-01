---- cria as roleplays views

select 
        datekey as duedatekey,
      [orderdate] as duedate,			
      [weekday_start]		,
      [newxtweek_startat]	,
      [nextweek_fromdate]	,
      [firstmondayinmonth]	,
      [lastmondayinmonth]	,
      [orderyear]			,
      [monthname]			,
      [monthabrv]			,
      [monthnum]			,
      [quartermonth]		,
      [yearmonth]			,
      [yearmonthnum]		,
      [lastdayofmonth]		,
      [weekname]			,
      [weeknameabrv]		,
      [isweekday]			,
      [daynum]				,
      [isweekbegin]			,
      [ismonthbegin]		,
      [islastdayofmonth]	,
      [leapyear]
from {{ ref ('ddate') }};