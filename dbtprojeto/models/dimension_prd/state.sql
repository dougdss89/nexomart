with elt_dimstate as (

    select

        stateprovincekey,
        territoryid,
        countryregioncode,
        stateprovinceid,
        stateprovincecode,
        [name]
    
    from {{ ref ('dimstateprovince') }}
) ,

normalize_table as( 
    select

        cast(stateprovincekey as int) as statekey,
        cast(territoryid as smallint ) as countryid,
        cast(countryregioncode as varchar(5)) as countrycode,
        cast(stateprovinceid as int) as stateid,
        cast(stateprovincecode as char(5)) as statecode,
        cast(name as nvarchar(30)) as statename

    from elt_dimstate
)

select * from normalize_table;