with elt_salesreason as (

    select 
        
        cast(salesreasonkey as int) as salesreasonkey,
        cast(salesreasonid as smallint) as salesreasonid,
        cast(salesreason as varchar(30)) as salesreason,
        cast(reasontype as varchar(15)) as reasontype

    from {{ ref ('dim_DDsalesreason') }}
)

select * from elt_salesreason;