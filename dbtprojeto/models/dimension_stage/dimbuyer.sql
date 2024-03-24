

with buyer_table as (
select distinct
	hre.businessentityid,
	pp.firstname,
	pp.lastname,
	hre.loginid,
	hre.birthdate,
	hre.maritalstatus,
	hre.gender,
	hre.hiredate,
	hre.jobtitle,
	hrd.departmentid,
	hrd.[name],
	hrd.groupname,
	hpe.payfrequency,
	hpe.rate,
	hpe.ratechangedate,
	hrdh.startdate,
	hrdh.enddate,
	hre.salariedflag,
	hre.currentflag,
	ROW_NUMBER() over (partition by hre.businessentityid
						order by rate desc) as rn
	
from {{ source ('humanres_src_schema', 'employee') }} as hre
	left join
	{{ source ('humanres_src_schema', 'employeepayhistory') }} as hpe
on hre.businessentityid = hpe.businessentityid
	left join
	{{ source ('humanres_src_schema', 'employeedepartmenthistory') }} as hrdh
on hre.businessentityid = hrdh.businessentityid
	left join
	{{ source ('person_src_schema', 'person') }} as pp
on hre.businessentityid = pp.businessentityid
	left join
	{{ source ('humanres_src_schema', 'department') }} as hrd
on hrd.departmentid = hrdh.departmentid
where hre.jobtitle = 'buyer' or hre.jobtitle like 'purchasing%')

select * from buyer_table
where rn = 1;