
with max_reasonid as (

	select 

		salesorderid,
		max(salesreasonid) as reasonid
        
	from {{ source ('salestables_src_schema', 'SalesOrderHeaderSalesReason') }}
	group by SalesOrderID),

salesreason as (

	select 

		salesreasonid,
		name,
		reasontype

	from {{ source ('salestables_src_schema', 'salesreason') }} ),

joinreason as(

	select
		salesorderid,
		reasonid,
		name as reasonname,
		reasontype
	from max_reasonid as mrs
		inner join
		salesreason as srs
	on mrs.reasonid = srs.salesreasonid)

select * from joinreason;