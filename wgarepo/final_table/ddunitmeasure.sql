with elt_unitmeasure as (

    select

        cast(unitmeasurekey as smallint) as unitmeasurekey,
        cast(unitmeasurecode  as varchar(5)) as unitcode,
        cast(unitmeasurename as varchar(25)) as measurename

    from {{ref ('dim_DDunitmeasure') }}
)

select * from elt_unitmeasure;