with elt_warehouse as (

    select

        cast(warehousekey as int) as warehousekey,
        cast(locationid as int) as locationid,

        case 
            when shelf = 'N/A' then 'Not Av'
            else shelf
        end as shelf,

        cast(bin as smallint) as bin,
        cast(quantity as int) as quantity,
        cast(productid as int) as productid,
        cast([availability] as numeric(12,2)) as available,
        cast(costrate as numeric(12,2)) as costrate,
        cast([name] as nvarchar(30)) as shelfname,
        cast(coalesce(billofmaterialsid, 999) as int) as billofmaterialsid,
        cast(coalesce(componentid, 999) as int) as componentid,
        cast(bomlevel as smallint) as bomlevel,
        cast(coalesce(perassemblyqty, 999) as int) as perassemblyqty,
        cast(coalesce(unitmeasurecode, 'Not Av') as char(5)) as unitmeasurecode,
        cast(coalesce(startdate, '9999-12-31') as date) as startdate,
        cast(coalesce(enddate, '9999-12-31') as date) as enddate

    from {{ ref('dimwarehouse') }}

        
)

select * from elt_warehouse;