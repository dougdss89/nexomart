
-- a coluna employeeid se liga com a coluna businessentityid da tabela buyer
-- a coluna vendorid se liga com supplier
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
    
from {{ source ('purch_src_schema', 'PurchaseOrderDetail' ) }} as pod

inner join

	{{ source ('purch_src_schema', 'PurchaseOrderHeader' ) }} as poh

on pod.PurchaseOrderDetailID = poh.PurchaseOrderID;

