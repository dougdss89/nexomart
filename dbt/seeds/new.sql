with load_purchase_fct as (
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
		fcp.employeeid,
		sdb.buyerkey,
		fcp.vendorid,
		sds.supplierkey,
		sdp.productid,
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

	from {{ ref ('fact_purchase') }} as fpc

        left join

        {{ ref ('ddate') }} as ddt
    on fpc.orderdate = ddt.orderdate

        left join

        {{ ref ('vw_duedate') }} as vdt

    on fpc.duedate = vdt.duedate

        left join

        {{ ref ('vw_shipmentdate' ) }} as vsp

    on fcp.shipdate = vsp.shipdate

        left join

        {{ ref ('dimbueyer' ) }} as ddb
    
    on fpc.employeeid = ddb.buyerid

        left join

        {{ ref ('supplier_company') }} as dsc

    on fpc.vendorid = dsc.supplierid

        left join

        {{ ref ('product') }} as dpp
    on fpc.productid = dpp.productid

        left join

        {{ ref ('product_discontinued') }} as dpc
    on fpc.productid = dpc.productid

        left join

        {{ ref ('freight' )}} as dfg
    on fpc.shipmethodid = dfg.shipid
)

select * from load_purchase_fct;