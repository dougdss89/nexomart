
-- a coluna employeeid se liga com a coluna businessentityid da tabela buyer
-- a coluna vendorid se liga com supplier
select 

	pod.PurchaseOrderID,
	pod.PurchaseOrderDetailID,
	poh.revisionnumber,
	poh.[status],
	poh.employeeid,
	poh.vendorid,
	cast(poh.orderdate as date) as orderdate,
	cast(poh.shipdate as date) as shipdate,
	cast(pod.duedate as date) as duedate,
	shipmethodid,
	pod.productid,
	cast(pod.receivedqty as int) as receivedqty,
	cast(pod.rejectedqty as int) as rejectedqty,
	cast(pod.stockedqty as int) as stockedqty,
	cast(pod.unitprice as decimal(12, 2)) as unitprice,
	cast(poh.freight as decimal(12, 2)) as freight,
	cast(poh.taxamt as decimal(12, 2)) as taxamt,
	cast(pod.linetotal as decimal(12, 2)) as linetotal,
	cast(poh.subtotal as decimal(12, 2)) as subtotal,
	cast(poh.totaldue as decimal(12, 2)) as totaldue
    
from {{ source ('purch_src_schema', 'PurchaseOrderDetail' ) }} as pod

inner join

	{{ source ('purch_src_schema', 'PurchaseOrderHeader' ) }} as poh

on pod.PurchaseOrderDetailID = poh.PurchaseOrderID;

