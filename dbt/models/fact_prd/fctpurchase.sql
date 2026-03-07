-- create purchase fact table from source
-- starting with select of columns to clean and prepare

-- creating a cte to limit the columns to work

with load_purchase_fct as (

	select 
		
		factpurchasekey,
		PurchaseOrderID,
		PurchaseOrderDetailID,
		ddt.orderdate,
		fpc.shipdate,
		vdt.duedate,
		ddt.datekey,
		vsp.shipdatekey,
		vdt.duedatekey,
		fpc.employeeid,
		ddb.buyerkey,
		fpc.vendorid,
		dsc.supplierkey,
		fpc.productid,
		dpp.productkey,
		dpc.productkey as prod_disc_key,
		fpc.shipmethodid,
		dfg.freightkey,
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

    on fpc.shipdate = vsp.shipdate

        left join

        {{ ref ('buyer' ) }} as ddb
    
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
),

-- normalize and remove nulls from source data
normalize_purchase as (

	select
		
		coalesce(factpurchasekey, 0) as factpurchasekey,  
		coalesce(PurchaseOrderID, 0) as PurchaseOrderID, 
		coalesce(PurchaseOrderDetailID, 0) as PurchaseOrderDetailID, 
		coalesce(orderdate, '9999-12-31') as orderdate, 					
		coalesce(shipdate, '9999-12-31') as shipdate,	
		coalesce(duedate, '9999-12-31') as duedate,	
		coalesce(datekey, 0) as datekey,
		coalesce(shipdatekey, 0) as shipdatekey,
		coalesce(duedatekey, 0) as duedatekey,
		coalesce(employeeid, 0) as employeeid,
		coalesce(buyerkey, 0) as buyerkey,
		coalesce(vendorid, 0) as vendorid,
		coalesce(supplierkey, 0) as supplierkey,
		coalesce(productid, 0) as productid,
		coalesce(productkey, 0) as productkey,
		coalesce(prod_disc_key, 0) as product_disc_key,	
		coalesce(shipmethodid,0) as shhipmethodid,
		coalesce(freightkey, 0) as freightkey,
		coalesce(receivedqty, 0) as receivedqty,
		coalesce(rejectedqty, 0) as rejectedqty,
		coalesce(stockedqty, 0) as stockedqty,
		coalesce(unitprice, 0.00) as unitprice,
		coalesce(freight, 0.00) as freight,
		coalesce(taxamt, 0.00) as taxamt,
		coalesce(linetotal, 0.00) as linetotal,
		coalesce(subtotal, 0.00) as subtotal,
		coalesce(totaldue, 0.00) as totaldue
	
    from load_purchase_fct
)

-- final select.

select * from normalize_purchase;
