
select

	coalesce (psp.TerritoryID, 0) territoryid,
	psp.CountryRegionCode,
	coalesce(psp.StateProvinceID, 0) as stateprovinceid,
	psp.StateProvinceCode,
	psp.name

from {{ source ('person_src_schema', 'stateprovince') }} as psp;
