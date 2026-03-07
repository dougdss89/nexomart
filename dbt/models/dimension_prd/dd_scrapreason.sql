with elt_scrap as (

    select

        cast(coalesce([scrapeasonkey], 0) as int) as scrapkey,
        cast(scrapreasonid as smallint) as scrapid,
        cast(scrapname as varchar(40)) as scrapname

    from {{ref ('dimscrapreason') }}
)

select * from elt_scrap;