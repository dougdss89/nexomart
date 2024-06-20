with elt_buyer as (

select
	
	*

from {{ ref('dimbuyer') }} ),

max_rate as(

select 

	businessentityid,
	MAX(rate) as rate,
	MAX(ratechangedate) as startdate

from elt_buyer
group by businessentityid),

join_maxrate as (

	select distinct
		CAST(buyerkey as int) as buyerkey,
		cast(mxrate.businessentityid as int) as buyerid,
		cast(firstname as nvarchar(20)) as firstname,
		cast(lastname as nvarchar(20)) as lastname,
		cast(birthdate as date) as birthdate,
		cast(datediff(yy,year(getdate()), birthdate) as smallint) as age,
		
		case
		when gender = 'f' then 'Female'
		else 'male'
		end as gender,

		cast(hiredate as date) as hiredate,
		cast(datediff(yy, year(getdate()), hiredate) as smallint) as yearsincompany,
		cast(jobtitle as varchar(20)) as jobtitle,
		cast([name] as varchar(20)) as departmentname,
		cast(groupname as varchar(20)) as division,
		cast(payfrequency as smallint) as payfrequency,
		CAST(mxrate.rate as numeric(12,2)) as rate,
		CAST(ratechangedate as date) as ratechangedate,
		cast(mxrate.startdate as date) as startdate,
		cast(enddate as date) as enddate,
		cast(salariedflag as bit) as issalaried,
		cast(currentflag as bit) as iscurrent

	from max_rate as mxrate

		inner join

		elt_buyer as elb
	on mxrate.businessentityid = elb.businessentityid)

select 

    distinct * 

from join_maxrate
where enddate is null 
and ratechangedate = startdate;
