{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimfreight] 
				  		     add freightkey int not null identity (1, 1)"}
    )
}}

select 

    coalesce(shipmethodid, 0) as shipmethodid,
    [Name],
    shipbase,
    shiprate

from {{ source ('purchasing_src_schema', 'shipmethod') }};
