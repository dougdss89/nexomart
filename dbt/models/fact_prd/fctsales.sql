with load_fact_sales as (

select 

	factsaleskey,
	salesorderid,
	salesorderdetailid,
	--sls.accountnumber,
	sls.orderdate,
	ddate.datekey,
	sls.shipdate,
	sls.duedate,
	sls.salespersonid,
	salespersonkey,
	sls.productid,
	productkey,
	sls.customerid,
	customerkey,
	dst.storekey,
	sls.shipmethodid,
	freightkey,
	sls.territoryid,
	countrykey,
	sls.specialofferid,
	promotionkey,
	sls.currencyrateid,
	currencykey,
	orderqty,
	unitprice,
	unitpricediscount,
	taxamt,
	freight,
	linetotal,
	onlineorderflag

from {{ref ('fact_sales') }} as sls
	
	left join
	
	{{ref ( 'product' )}} as prd

on sls.productid = prd.productid

	left join

	{{ ref ('freight') }} as dfr
on sls.shipmethodid = dfr.shipid

	left join

	{{ ref('salesperson') }} as dsps
on sls.salespersonid = dsps.salespersonid

	left join

	{{ ref('customer') }} as dcs
on sls.customerid = dcs.customerid

	left join

	{{ ref ('promotion') }} as dpm
on sls.specialofferid = dpm.promotionid 

	left join

	{{ ref('currency_rate') }} as dcr
on sls.currencyrateid = dcr.currencyrateid

	left join

	{{ ref('ddate') }} as ddate
on sls.orderdate = ddate.orderdate

	left join

	{{ ref('country') }} as dct
on sls.territoryid = dct.countryid

	left join

	{{ ref('store') }} as dst
on sls.customerid = dst.customerid

),

fact_elt as (

select

    factsaleskey,
	cast(SalesOrderID as int) as salesorderid,
	cast(SalesOrderDetailID as int) as detailorderid,
	cast(OrderDate as date) as orderdate,
	cast(datekey as int) as datekey,
	cast(ShipDate as date) as shipdate,
	cast(DueDate as date) as duedate,
	cast(coalesce(SalesPersonID, 0) as int) as salespersonid,
	cast(coalesce(salespersonkey, 0) as int) as salespersonkey,
	cast(coalesce(ProductID, 0) as int) as productid,
	cast(coalesce(productkey, 0) as int) as productkey,
	cast(coalesce(CustomerID, 0) as int) as customerid,
	cast(coalesce(customerkey, 0) as int) as customerkey,
	cast(coalesce(storekey, 0) as int) as storekey,
	cast(coalesce(ShipMethodID, 0) as tinyint) as shippingid,
	cast(coalesce(freightkey, 0) as int) as freightkey,
	cast(coalesce(TerritoryID, 0) as tinyint) as territoryid,
	cast(coalesce(countrykey, 0) as int) as countrykey,
	cast(coalesce(SpecialOfferID, 0) as tinyint) as promotionid,
	cast(coalesce(promotionkey, 0) as int) as promotionkey,
	cast(coalesce(currencyrateid, 0) as int) as currencyrateid,
	cast(coalesce(currencykey, 0) as int) as currencykey,
	cast(orderqty as int) as quantity,
	cast(unitprice as numeric(12,2)) as unitprice,
	cast(unitpricediscount as numeric(12,3)) as discountprice,
	cast(taxamt as numeric(9,2)) as taxes,
	cast(freight as numeric(9,2)) as freight,
	cast(LineTotal as numeric(12,2)) as linetotal,
	cast(OnlineOrderFlag as tinyint) as isolineorder
    
from load_fact_sales
)

select * from fact_elt;

