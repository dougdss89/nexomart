with elt_dimcountry as (

    select

        countryregionkey,
        territoryid,
        [group],
        [name],

        case
            when [name] = 'Northwest' then 'NW'
            when [name] = 'Northeast' then 'NE'
            when [name] = 'Southwest' then 'SW'
            when [name] = 'Southeast' then 'SE'
            else 'CNT'
	    end as regioncode,

        countryregioncode,

        case
			when CountryRegionCode = 'US' then 'United States'
			when CountryRegionCode = 'CA' then 'Canada'
			when CountryRegionCode = 'FR' then 'France'
			when CountryRegionCode = 'GB' then 'United Kingdom'
			when CountryRegionCode = 'AU' then 'Australia'
			when CountryRegionCode = 'DE' then 'Germany'
		else CountryRegionCode 
		end as 'countryname'

    from {{ ref('dimcountryregion') }}

),

normalize_table as (

    select

        cast(countryregionkey as int) as countrykey,
        cast(territoryid as smallint) as countryid,
        cast([group] as varchar(15)) as continentname,
        cast(regioncode as char(3)) as regioncode,
        cast([name] as varchar(15)) as regionname,
        cast(countryregioncode as char(3)) as countrycode,
        cast(countryname as varchar(15)) as countryname

    from elt_dimcountry

)

select * from normalize_table;