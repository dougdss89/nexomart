{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[fact_production] 
				  		     add fctproductionkey int not null identity (1, 1)"}
    )
}}

select

	pw.workorderid,
	pw.productid,
	pw.orderqty,
	pw.stockedqty,
	pw.scrappedqty,
	pw.scrapreasonid,
	pwr.locationid,
	pwr.actualresourcehrs,
	pwr.plannedcost,
	pwr.actualcost,
	pwr.scheduledstartdate,
	pwr.scheduledenddate,
	pwr.actualstartdate,
	pwr.actualenddate,
	pw.startdate,
	pw.enddate,
	pw.duedate
	
from {{ source ('production_src_schema', 'workorder' ) }} as pw
left join
	{{ source ('production_src_schema', 'workorderrouting' ) }} as pwr
on pw.workorderid = pwr.workorderid