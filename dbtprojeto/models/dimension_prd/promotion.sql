
--tratamento da tabela promotion
with elt_promo as (

select 
    promotionkey,
    specialofferid as promotionid,
    [description] as promotiondescription,
    discountpct as discountpercent,
    [type] as promotiontype,
    category as promocategory,
    minqty,

    case
        when maxqty is null and [type] like 'No disc%' then 0
        when maxqty is null and MinQty > 60 then 70
        when maxqty is null and  [type] like 'discontinued%' then 1000
        when MaxQty is null and [type] like 'excess%' then 1000
        when maxqty is null and [type] like 'seasonal%' then 100
        when maxqty is null and [type] like 'new pro%' then 10
    else maxqty
    end as maxqty,

    cast(startdate as date) as startdate,
    cast(enddate as date) as enddate,

    case 
        when enddate is not null 
            and enddate >= (select max(cast(orderdate as date)) 
                            from {{ ref ('fact_sales') }}) 
            or [type] not like 'discontinued%' then 'Yes'
        else 'No'
    end as ispromoactive

from {{ ref('dimpromotion') }}
),

-- define os datatypes corretos pro dw

define_datatypes as (

    select

        cast(promotionkey as int) as promotionkey,
        cast(promotionid as int) as promotionid,
        cast(promotiondescription as varchar(40)) as promotiondescription,
        cast(discountpercent as numeric(12,2)) as discountpercent,
        cast(promotiontype as varchar(25)) as promotiontype,
        cast(promocategory as varchar(15)) as promocategory,
        cast(minqty as smallint) as minquantity,
        cast(maxqty as smallint) as maxquantity,
        startdate,
        enddate,
        cast(ispromoactive as char(5)) as ispromoactive
    
    from elt_promo
)

select * from define_datatypes;
