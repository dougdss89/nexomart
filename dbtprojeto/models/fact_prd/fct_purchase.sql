-- create purchase fact table from source
-- starting with select of columns to clean and prepare

-- creating a cte to limit the columns to work

with load_purchase_key as (

	select
		
		factpurchasekey,
		PurchaseOrderID,
		PurchaseOrderDetailID,
		fcp.orderdate,
		ssp.shipmentdate,
		svd.duedate,
		sdd.datekey,
		ssp.shipdatekey,
		svd.duedatekey,
		sdb.buyerkey,
		sds.supplierkey,
		fcp.productid,
		sdp.productkey,
		fcp.shipmethodid,
		sdf.freightkey,
		receivedqty,
		rejectedqty,
		stockedqty,
		unitprice,
		freight,
		taxamt,
		linetotal,
		subtotal,
		totaldue


	from {{ref ('fact_purchase') }} as fcp

		left join

		{{ ref ('dimbuyer') }} as sdb
	on fcp.employeeid = sdb.businessentityid

		left join

		{{ ref('supplier_company') }} as sds
	on fcp.vendorid = sds.businessentityid

		inner join
		{{ ref ('dimdate') }} as sdd
	on fcp.orderdate = sdd.orderdate

		inner join

		{{ ref ('vwshipmentdate') }} as ssp
	on fcp.shipdate = ssp.orderdate
		
		inner join

		{{ ref ('vwduedate') }} as svd
	on fcp.duedate = svd.duedate

		left join

		{{ ref ('dimproduct') }} as sdp
	on fcp.productid = sdp.productid

		left join

		{{ ref ('dimfreight') }} as sdf
	on fcp.shipmethodid = sdf.shipmethodid
	)

-- normalize and remove nulls from source data
normalize_purchase as (

	select
		
		factpurchasekey,
		PurchaseOrderID as purchaseirderid,
		PurchaseOrderDetailID as purchasedetailid,
		fcp.orderdate,
		ssp.shipmentdate,
		svd.duedate,
		sdd.datekey,
		ssp.shipdatekey,
		svd.duedatekey,
		sdb.buyerkey,
		sds.supplierkey,
		fcp.productid,
		sdp.productkey,
		fcp.shipmethodid,
		sdf.freightkey,
		receivedqty,
		rejectedqty,
		stockedqty,
		unitprice,
		freight,
		taxamt,
		linetotal,
		subtotal,
		totaldue

    from load_purchase_key
)

-- final select.

select * from normalize_purchase;
