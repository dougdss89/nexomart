
with elt_dimdate as (

select

	distinct(cast(orderdate as date)) as orderdate

from {{ source ('salestables_src_schema', 'salesorderheader') }}

),

cria_dimdate as (

select

	cast(orderdate as date) as datekey,
	cast(orderdate as date) as orderdatekey,
	cast(orderdate as date) as orderdate,
	year(orderdate) as orderyear,
	'FY' + cast(year(orderdate) as varchar(5)) as fiscalyear,
	datename(mm,orderdate) as monthname,
	substring(cast(datename(m, orderdate) as varchar(10)),1,3) as monthabrv,
	datepart(m,orderdate) as monthnum,

	case
		when datepart(m, orderdate) between 1 and 3 then 'Q1'
		when datepart(m, orderdate) between 4 and 6 then 'Q2'
		when datepart(m, orderdate) between 6 and 9 then 'Q3'
		when datepart(m, orderdate) between 9 and 12 then 'Q4'
	end as quartermonth,

	datename(w, orderdate) as weekname,
	substring(cast(datename(w, orderdate) as varchar(10)),1,3) as weeknameabrv,

	case 
		when cast(datename(w, orderdate) as varchar(10)) = 'Saturday' or cast(datename(w, orderdate) as varchar(10)) = 'Sunday' then 'Weekend'
		else 'Weekday'
	end as isweekday,

	datename(ww, orderdate) as weeknuminyear,
	datepart(d, orderdate) as daynum,

	case 
		when datepart(d, orderdate) = 1 then 'Yes'
		else 'No'
	end as ismonthbegin,

	case
		when cast(datename(w, orderdate) as varchar(10)) = 'monday' then 'Yes'
		else 'No'
	end as isweekbegin,
	
	case
		when cast(datename(w, orderdate) as varchar(3)) like 'mon%' then cast(orderdate as date)
	end as weekbegindate,

	upper(cast(year(orderdate) as varchar(5)) + '_' + substring(cast(datename(m, orderdate) as varchar(3)),1,3)) as yearmonth,
	cast(datepart(YYYY, orderdate) as varchar(5)) + '' +  cast(month(orderdate) as varchar(5)) as yearmonthnum,
	eomonth(orderdate) as lastdayofmonth,

	case
		when datepart(d, orderdate) = datepart(d, eomonth(orderdate)) then 'Yes'
		--when datepart(m, orderdate) = 2 and datepart(d, orderdate) between 28 and 29 then 'Yes'
	else 'No'
	end as islastdayofmonth,

	case	
		when year(orderdate) % 4 = 0 or year(orderdate) % 400 = 0 and year(orderdate) % 100 <> 0  then 'Yes'
	else 'No'
	end as leapyear

from elt_dimdate),

normaliza_data as(

	select 
		datekey,
		orderdatekey,
		orderdate,
		orderyear,
		monthname,
		monthabrv,
		monthnum
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
	from cria_dimdate)

select * from normaliza_data