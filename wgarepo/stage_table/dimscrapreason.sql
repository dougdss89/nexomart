{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimscrapreason] 
				  		     add scrapeasonkey int not null identity (1, 1)"}
    )
}}

select

	coalesce(scrapreasonid,0) as scrapreasonid,
	[name] as scrapname
     
from {{ source ('production_src_schema', 'scrapreason') }};