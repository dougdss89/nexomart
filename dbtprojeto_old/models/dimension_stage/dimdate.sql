
with elt_dimdate as (

select

	distinct(cast(orderdate as date)) as orderdate

from {{ source ('salestables_src_schema','salesorderheader') }}

),

cria_dimdate as (
 
select

	cast(orderdate as date) as datekey,
	cast(orderdate as date) as orderdatekey,
	cast(orderdate as date) as orderdate,

	cast(dateadd(day,
			datediff(day, '19000101', orderdate) / 7 * 7, '19000101') as date) as weekday_start,

	cast(dateadd(day,
			datediff(day, '19000101', orderdate) / 7 * 7 + 7, '19000101') as date) as newxtweek_startat,
	case
		when datename(weekday, orderdate) = 'monday' then cast(dateadd(day,
			datediff(day, '19000101', orderdate) / 7 * 7 + 7, '19000101') as date)

		when datename(weekday, orderdate) = 'tuesday' then cast(dateadd(day,
			datediff(day, '19000101', orderdate) / 7 * 7 + 7, '19000102') as date)

		when datename(weekday, orderdate) = 'wednesday' then cast(dateadd(day,
			datediff(day, '19000101', orderdate) / 7 * 7 + 7, '19000103') as date)

		when datename(weekday, orderdate) = 'thursday' then cast(dateadd(day,
			datediff(day, '19000101', orderdate) / 7 * 7 + 7, '19000104') as date)

		when datename(weekday, orderdate) = 'friday' then cast(dateadd(day,
			datediff(day, '19000101', orderdate) / 7 * 7 + 7, '19000105') as date)

		when datename(weekday, orderdate) = 'saturday' then cast(dateadd(day,
			datediff(day, '19000101', orderdate) / 7 * 7 + 7, '19000106') as date)

		when datename(weekday, orderdate) = 'sunday' then cast(dateadd(day,
			datediff(day, '19000101', orderdate) / 7 * 7 + 7, '19000107') as date)
	end as nextweek_fromdate,

	cast(dateadd(day, datediff(day,'19000101',
							dateadd(month, datediff(month, '19000101', orderdate), '19000101') - 1)/7 * 7 + 7, '19000101') as date) as firstmondayinmonth,

-- como é a útlima segunda, precisa da última data do ano 
	cast(dateadd(day, datediff(day, '19000101',
							dateadd(month, datediff(month, '18991231', orderdate), '18991231')) /7 * 7, '19000101') as date) as lastmondayinmonth,


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