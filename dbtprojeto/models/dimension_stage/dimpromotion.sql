{{ 
    config(

        post_hook = {"sql": "alter table [stage_dim].[dimpromotion] 
				  		     add promotionkey int not null identity (1, 1)"}
    )
}}

select 

    coalesce(specialofferid, 0) as specialofferid,
    [description],
    discountpct,
    [type],
    category,
    minqty,
    maxqty,
    startdate,
    enddate

from {{ source ('salestables_src_schema', 'specialoffer') }};
