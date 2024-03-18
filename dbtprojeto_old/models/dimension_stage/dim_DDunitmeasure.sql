
select 
	unitmeasurecode,
	[name] as unitmeasurename

from {{ source ('production_src_schema', 'unitmeasure') }}