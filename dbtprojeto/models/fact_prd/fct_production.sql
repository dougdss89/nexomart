
-- create workorder fact table
-- first step is cleaning data, null values and set data type

with elt_workorder as (

    select

        cast(fctproductionkey as int) as fctproductionkey,
        cast(workorderid as int) as workorderid,
        cast(productid as int) as productid,
        cast(orderqty as int) as orderqty,
        cast(stockedqty as int) as stockedqty,
        cast(scrappedqty as int) as scrappedqty,
        cast(coalesce(scrapreasonid, 999) as int) as scrapreasonid,
        cast(coalesce(locationid, 999) as int) as locationid,
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

)

-- now, select columns from cte to persist in database
-- since i'am selected from a cte, select * isn't a problem.

select * from elt_workorder;
 