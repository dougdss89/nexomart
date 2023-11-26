{{
	config (

		post_hook= {"sql": "alter table [db_stage].[dim_ddunitmeasure] 
				  		 add unitmeasurekey int not null identity (1, 1)"}
	)

}}

select 
	unitmeasurecode,
	[name] as unitmeasurename

from {{ source ('production_src_schema', 'unitmeasure') }}