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

    on fpc.shipdate = vsp.shipmentdate

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
		
		factpurchasekey,
		PurchaseOrderID,
		PurchaseOrderDetailID,
		orderdate,
		shipdate,
		duedate,
		datekey,
		shipdatekey,
		duedatekey,
		employeeid,
		buyerkey,
		vendorid,
		supplierkey,
		productid,
		productkey,
		prod_disc_key,
		shipmethodid,
		freightkey,
		receivedqty,
		rejectedqty,
		stockedqty,
		unitprice,
		freight,
		taxamt,
		linetotal,
		subtotal,
		totaldue

    from load_purchase_fct
)

-- final select.

select * from normalize_purchase;
