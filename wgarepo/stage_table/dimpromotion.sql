{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimpromotion] 
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
