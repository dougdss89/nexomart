
select

	distinct
	soh.SalesOrderID,
	purchaseordernumber,
	carriertrackingnumber,
	accountnumber,
	salesordernumber,
	CreditCardApprovalCode,
	soh.OnlineOrderFlag
    
from {{ source ('salestables_src_schema', 'salesorderheader') }} as soh

	left join

	{{ source ('salestables_src_schema', 'salesorderdetail') }} as sod

on soh.SalesOrderID = sod.SalesOrderID;