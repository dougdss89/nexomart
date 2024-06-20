{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[fact_sales] 
				  		     add fct_saleskey int not null identity (1, 1)"}
    )
}}

select

	sso.SalesOrderID,
	sso.SalesOrderDetailID,
	accountnumber,
	carriertrackingnumber,
	purchaseordernumber,
	salesordernumber,
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

from {{source ('salestables_src_schema', 'salesorderdetail')}} as sso

left join

    {{source ('salestables_src_schema', 'salesorderheader')}} as soh
on sso.SalesOrderID = soh.SalesOrderID

left join
	{{ source ('salestables_src_schema', 'SalesOrderHeaderSalesReason')}} sohs

on soh.salesorderid = sohs.salesorderid