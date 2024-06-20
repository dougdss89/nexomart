{{ config (

    post_hook = {"sql": "alter table [db_stage].[dim_ddtransactioncode] 
				  		 add transactioncodekey int not null identity (1, 1)"}
) }}

select

	distinct
	soh.SalesOrderID,
	purchaseordernumber,
	salesordernumber,
	carriertrackingnumber,
	accountnumber,
	CreditCardApprovalCode,
	soh.OnlineOrderFlag
    
from {{ source ('salestables_src_schema', 'salesorderheader') }} as soh
	left join
	{{ source ('salestables_src_schema', 'salesorderdetail') }} as sod
on soh.SalesOrderID = sod.SalesOrderID;