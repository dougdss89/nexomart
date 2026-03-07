
-- create workorder fact table
-- first step is cleaning data, null values and set data type

with elt_workorder as (

    select

        cast(factproductionkey as int) as fctproductionkey,
        cast(workorderid as int) as workorderid,
        cast(productid as int) as productid,
        cast(orderqty as int) as orderqty,
        cast(stockedqty as int) as stockedqty,
        cast(scrappedqty as int) as scrappedqty,
        cast(coalesce(scrapreasonid, 0) as int) as scrapreasonid,
        cast(coalesce(locationid, 0) as int) as locationid,
        cast(coalesce(actualresourcehrs, 999.00) as numeric(12,2)) as actualresourcehrs,
        cast(coalesce(plannedcost, 999.99) as numeric(12,2)) as plannedcost,
        cast(coalesce(actualcost, 999.99) as numeric(12,2)) as actualcost,
        cast(coalesce(scheduledstartdate, '9999-12-31') as date) as scheduledstartdate,
        cast(coalesce(scheduledenddate, '9999-12-31') as date) as scheduledenddate,
        cast(coalesce(actualstartdate, '9999-12-31') as date) as actualstartdate,
        cast(coalesce(actualenddate, '9999-12-31') as date) as actualenddate,
        cast(coalesce(startdate,  '9999-12-31') as date) as startdate,
        cast(coalesce(enddate, '9999-12-31') as date) as enddate,
        cast(coalesce(duedate, '9999-12-31') as date) as duedate
    
    from {{ ref('fact_production') }}

),

load_production_key as(

    SELECT [fctproductionkey]   ,
       [workorderid]		,
	   datekey				,
       [scheduledstartdate]	,
       [scheduledenddate]	,
       [actualstartdate]	,
       [actualenddate]		,
       [startdate]			,
       [enddate]			,
       [duedate]			,
       ewo.[productid]		,
	   coalesce(pd.productkey, 0) as productkey,
	   coalesce(pds.productkey, 0) as prod_disc_key,
       [scrapreasonid]		,
	   coalesce(scp.scrapkey, 0) as scrapkey,
       [locationid]			,
       [orderqty]			,
       [stockedqty]			,
       [scrappedqty]		,
       [actualresourcehrs]	,
       [plannedcost]		,
       [actualcost]			

from elt_workorder as ewo

	left join

	{{ ref ( 'product' ) }} as pd
on ewo.productid = pd.productid

	left join

	{{ref ( 'product_discontinued' )}} as pds
on ewo.productid = pds.productid

	left join

	{{ ref ( 'dd_scrapreason' )}} as scp
on ewo.scrapreasonid = scp.scrapid

	left join

	{{ ref ( 'ddate' )}} as ddt
on ewo.startdate = ddt.orderdate

)

-- now, select columns from cte to persist in database
-- since i'am selected from a cte, select * isn't a problem.

select * from load_production_key;
 