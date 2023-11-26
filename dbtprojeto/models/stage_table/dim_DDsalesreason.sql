{{ config (

    post_hook = {"sql": "alter table [db_stage].[dim_ddsalesreason] 
				         add salesreasonkey int not null identity (1, 1)"}
) }}

select 
	coalesce(jsr.SalesReasonID, 0) as salesreasonid,
    [Name] as salesreason,
    soh.salesorderid,
    ReasonType

from {{ source ('salestables_src_schema', 'salesreason') }} as jsr

    left join

    {{ source ('salestables_src_schema', 'SalesOrderHeaderSalesReason')}} as soh

    on jsr.salesreasonid = soh.salesreasonid

