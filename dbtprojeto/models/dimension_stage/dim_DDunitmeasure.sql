{{
	config (

		post_hook= {"sql": "alter table [stage_dim].[dim_ddunitmeasure] 
				  		 add unitmeasurekey int not null identity (1, 1)"}
	)

}}

select 
	unitmeasurecode,
	[name] as unitmeasurename

from {{ source ('production_src_schema', 'unitmeasure') }}