{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[fact_sales] 
				  		     add fct_saleskey int not null identity (1, 1)"}
    )
}}

select

	sso.SalesOrderID,
	sso.SalesOrderDetailID,
	soh.OrderDate,
	soh.ShipDate,
	soh.DueDate,
	SalesPersonID,
	ProductID,
	CustomerID,
	ShipMethodID,
	TerritoryID,
	SpecialOfferID,
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
on sso.SalesOrderID = soh.SalesOrderID;