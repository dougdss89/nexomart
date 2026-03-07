

select

	coalesce(scrapreasonid,0) as scrapreasonid,
	[name] as scrapname
     
from {{ source ('production_src_schema', 'scrapreason') }};