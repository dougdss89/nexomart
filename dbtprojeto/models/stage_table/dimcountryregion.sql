{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimcountryregion] 
				  		     add countryregionkey int not null identity (1, 1)"}
    )
}}
select distinct

    sst.territoryid,
    sst.[name],
    sst.[group],
    sst.countryregioncode

from {{ source ('salestables_src_schema', 'salesterritory') }} as sst

inner join
    {{ source ('person_src_schema', 'countryregion') }} as pcr

on pcr.countryregioncode = sst.countryregioncode

left join
    {{ source ('person_src_schema', 'stateprovince') }} as psp

on psp.countryregioncode = pcr.countryregioncode