with elt_buyer as (

select
	
	*

from {{ ref('dimbuyer') }} ),

join_maxrate as (

	select distinct
	
		CAST(coalesce(buyerkey, 0) as int) as buyerkey,
		cast(businessentityid as int) as buyerid,
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
		CAST(rate as numeric(12,2)) as rate,
		CAST(ratechangedate as date) as ratechangedate,
		cast(startdate as date) as startdate,
		cast(coalesce(enddate, '9999-12-31') as date) as enddate,
		cast(salariedflag as bit) as issalaried,
		cast(currentflag as bit) as iscurrent

	from elt_buyer )

	select * from join_maxrate