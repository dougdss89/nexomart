{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimcurrency_name] 
				  		     add currencyname_key int not null identity (1, 1)"}
    )
}}

select 
    scr.currencycode,
    scr.[name] as currencyname,
    crc.CountryRegionCode,
    pcr.[name] as countryname
from {{ source ('currencyname_src_schemasales', 'currency') }} as scr
    inner join
    {{ source ('currencyname_src_schemasales', 'countryregioncurrency') }} as crc
on scr.CurrencyCode = crc.CurrencyCode
    inner join
   {{ source ('person_src_schema', 'countryregion') }} as pcr
on crc.CountryRegionCode = pcr.CountryRegionCode