{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimstateprovince] 
				  		     add stateprovincekey int not null identity (1, 1)"}
    )
}}

select

	coalesce (psp.TerritoryID, 0) territoryid,
	psp.CountryRegionCode,
	coalesce(psp.StateProvinceID, 0) as stateprovinceid,
	psp.StateProvinceCode,
	psp.name

from {{ source ('person_src_schema', 'stateprovince') }} as psp;
