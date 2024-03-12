with elt_supplier as (

select 
    pv.supplierkey,
	pv.businessentityid,
	pv.accountnumber,
	pv.name as supplliername,
	ppv.productid,
	productname,

	case 
		when pv.creditrating = 1 then 'Premier'
		when pv.creditrating between 2 and 3 then 'Super class'
		when pv.creditrating = 4 then 'Normal'
		when pv.creditrating = 5 then 'Attention'
	else 'No Classification'
	end as ratingclass,

	pv.creditrating,

	case
		when pv.preferredvendorstatus = 1 then 'Yes'
		else  'No'
	end as preferredevendorstatus,


	pv.averageleadtime,
	pv.standardprice,
	cast(pv.lastreceiptcost as numeric(12,2)) as lastreceiptcost,
	cast(pv.lastreceiptdate as date) as lastreceiptdate,
	pv.minorderqty,
	pv.maxorderqty,
	coalesce(pv.onorderqty, 0) as onorderqty,

	case 
		when pv.unitmeasurecode = 'ea' then 'Each'
		when pv.unitmeasurecode = 'cs' then 'Case'
		when pv.unitmeasurecode = 'pak' then 'Pack'
		when pv.unitmeasurecode = 'dz' then 'Dozen'
		when pv.unitmeasurecode = 'ctn' then 'Carton'
		when pv.unitmeasurecode = 'gal' then 'Galoon'
	else 'Unknown'
	end as unitmeasure

from {{ref ('dimsupplier') }} as pv
	inner join
	{{ref ('dimsupplier') }} as ppv
on pv.businessentityid = ppv.businessentityid
	inner join
	{{ ref ('dimproduct') }} as  pp
on pp.ProductID = ppv.ProductID),

normaliza_datatype as (

    select

        cast(supplierkey as int) as supplierkey,
        cast(businessentityid as int) as supplierid,
        cast(accountnumber as varchar(15)) as supplieraccount,
        cast(supplliername as varchar(40)) as supplliername,
        cast(productid as int) as productid,
        cast(productname as varchar(50)) as productname,
        cast(ratingclass as varchar(15)) as ratingclass,
        cast(creditrating as smallint) as creditrating,
        cast(preferredevendorstatus as char(5)) as preferredevendorstatus,
        cast(averageleadtime as numeric(12,2)) as averageleadtime,
        cast(standardprice as numeric(12,2)) as standardprice,
        lastreceiptcost, -- já normalizado,
        lastreceiptdate,-- já normalizado na cte anterior,
        cast(minorderqty as int) as minorderqty,
        cast(maxorderqty as int) as maxorderqty,
        cast(onorderqty as int) as onorderqty,
        cast(unitmeasure as varchar(15)) as unitmeasure

    from elt_supplier
)

select * from normaliza_datatype;