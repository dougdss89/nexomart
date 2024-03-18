-- create purchase fact table from source
-- starting with select of columns to clean and prepare

-- creating a cte to limit the columns to work

with elt_purchase as (

    select

        fctpurchasekey,
        purchaseorderid,
        purchaseorderdetailid,
        revisionnumber,
        [status],
        employeeid,
        vendorid,
        shipmethodid,
        productid,
        receivedqty,
        rejectedqty,
        stockedqty,
        unitprice,
        freight,
        taxamt,
        linetotal,
        subtotal,
        totaldue,
        orderdate,
        shipdate,
        duedate

    from {{ ref('fact_purchase') }}

),

-- normalize and remove nulls from source data
normalize_purchase as (

    select 

        cast(fctpurchasekey as int) as fpurchasekey,
        cast(purchaseorderid as int) as purchaseorderid,
        cast(purchaseorderdetailid as int) prcorderdetailid,
        cast(coalesce(revisionnumber, 999) as smallint) as revisionnumber,
        cast(coalesce([status], 999) as smallint) as purchasestatus,
        cast(coalesce(employeeid, 999) as smallint) as employeeid,
        cast(coalesce(vendorid, 999) as int) as vendorid,
        cast(coalesce(shipmethodid, 999) as smallint) as shipmethodid,
        cast(productid as int) as productid,
        cast(receivedqty as int) as receivedqty,
        cast(rejectedqty as int) as rejectedqty,
        cast(stockedqty as int) as stockedqty,
        cast(unitprice as numeric(12,2)) as unitprice,
        cast(coalesce(freight, 9999.99) as numeric(12,2)) as freight,
        cast(coalesce(taxamt, 999.99) as numeric(12,2)) as taxamt,
        cast(linetotal as numeric(12,2)) as linetotal,
        cast(coalesce(subtotal, 9999.99) as numeric(12,2)) as subtotal,
        cast(coalesce(totaldue, 9999.99) as numeric(12,2)) as totaldue,
        cast(coalesce(orderdate, '9999-12-31') as date) as orderdate,
        cast(coalesce(shipdate, '9999-12-31') as date) as shipdate,
        cast(coalesce(duedate, '9999-12-31') as date) as duedate

    from elt_purchase
)

-- final select.

select * from normalize_purchase;
