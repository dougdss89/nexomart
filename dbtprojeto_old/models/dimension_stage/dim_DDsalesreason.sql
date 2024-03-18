
select 

	coalesce(jsr.SalesReasonID, 0) as salesreasonid,
    [Name] as salesreason,
    ReasonType

from    {{ source ('salestables_src_schema', 'salesreason') }} as jsr