{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimsalesperson] 
				  		     add salespersonkey int not null identity (1, 1)"}
    )
}}


select

    pp.businessentityid,
    hre.loginid,
    pp.firstname,
    pp.lastname,
    hre.gender,
    hre.birthdate,
    hre.hiredate,
    hre.jobtitle,
    hre.maritalstatus,
    hre.organizationlevel,
    hreh.departmentid,
    hreh.startdate,
    hreh.enddate,
    hrd.groupname,
    hrd.[name] as departmentname,
    hreph.payfrequency,
    hreph.rate,
    hreph.ratechangedate,
    ssp.commissionpct,
    ssp.salesquota,
    ssp.bonus,
    sst.[name] as regionname,
    sst.[group] as continentname,
    sst.countryregioncode,
    sst.territoryid,
    salariedflag,
    currentflag

from {{ source ('person_src_schema', 'person') }} as pp
left join
    {{ source ('humanres_src_schema', 'employee') }} as hre
on pp.businessentityid = hre.businessentityid
left join
    {{ source ('humanres_src_schema', 'employeedepartmenthistory') }} as hreh
on hre.businessentityid = hreh.businessentityid
left join
    {{ source ('humanres_src_schema', 'department') }} as hrd
on hreh.departmentid = hrd.departmentid
left join
    {{ source ('humanres_src_schema', 'employeepayhistory') }} as hreph
on hreph.businessentityid = hre.businessentityid
inner join
    {{ source ('salestables_src_schema', 'salesperson') }} as ssp
on hre.businessentityid = ssp.businessentityid
left join
    {{ source ('salestables_src_schema', 'salesterritory') }} as sst
on sst.territoryid = ssp.territoryid;
