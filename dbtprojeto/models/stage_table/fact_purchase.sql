{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[fact_purchase] 
				  		     add fctpurchasekey int not null identity (1000, 1)"}
    )
}}

select 

	pod.PurchaseOrderID,
	pod.PurchaseOrderDetailID,
	poh.revisionnumber,
	poh.[status],
	poh.employeeid,
	poh.vendorid,
	poh.orderdate,
	poh.shipdate,
	pod.duedate,
	shipmethodid,
	pod.productid,
	pod.receivedqty,
	pod.rejectedqty,
	pod.stockedqty,
	pod.unitprice,
	poh.freight,
	poh.taxamt,
	pod.linetotal,
	poh.subtotal,
	poh.totaldue
    
from {{ source ('purchasing_src_schema', 'PurchaseOrderDetail' ) }} as pod

left join

	{{ source ('purchasing_src_schema', 'PurchaseOrderHeader' ) }} as poh
on pod.PurchaseOrderDetailID = poh.PurchaseOrderID;

