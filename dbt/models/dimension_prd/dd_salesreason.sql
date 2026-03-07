with elt_salesreason as (

    select 
        
        cast(coalesce(salesreasonkey, 0) as int) as salesreasonkey,
        cast(reasonid as smallint) as salesreasonid,
        cast(reasonname as varchar(30)) as salesreason,
        cast(reasontype as varchar(15)) as reasontype

    from {{ ref ('dim_DDsalesreason') }}
)

select * from elt_salesreason;