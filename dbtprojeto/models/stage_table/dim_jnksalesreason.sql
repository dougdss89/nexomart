{{ config (

    post_hook = {"sql": "alter table [db_stage].[dim_jnksalesreason] 
				         add salesreasonkey int not null identity (1, 1)"}
) }}

select 
	coalesce(SalesReasonID, 0) as salesreasonid,
    [Name] as salesreason,
    ReasonType

from {{ source ('salestables_src_schema', 'salesreason') }}