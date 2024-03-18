
select

	sso.SalesOrderID,
	sso.SalesOrderDetailID,
	accountnumber,
	soh.OrderDate,
	soh.ShipDate,
	soh.DueDate,
	sohs.salesreasonid,
	SalesPersonID,
	ProductID,
	CustomerID,
	ShipMethodID,
	TerritoryID,
	SpecialOfferID,
	currencyrateid,
	orderqty,
	unitprice,
	unitpricediscount,
	taxamt,
	freight,
	OnlineOrderFlag,
	LineTotal

from {{source ('sales_src_schema', 'salesorderdetail')}} as sso

left join

    {{source ('sales_src_schema', 'salesorderheader')}} as soh
on sso.SalesOrderID = soh.SalesOrderID

left join
	{{ source ('sales_src_schema', 'SalesOrderHeaderSalesReason')}} sohs

on soh.salesorderid = sohs.salesorderid