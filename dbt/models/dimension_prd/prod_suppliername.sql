with elt_suppliername as (
	
select 
	
	cast(coalesce(supplierkey, 0) 	as int) as supplierproduct_key,
	cast(ProductID	 	as int) as productid,
	cast(AccountNumber 	as varchar(15)) as supplieraccount,
	cast(supplierid 	as int) as supplierproduct_id,
	cast(companyname 	as varchar(40)) as companyname,
	cast(firstname 		as varchar(15)) as firstname,
	cast(lastname 		as varchar(15)) as lastname,
	cast(contacttype 	as varchar(25)) as contacttype,

	case
		when contacttype = 'Assistant Sales Agent' then 1
		when contacttype = 'Sales Agent' then 2
		when contacttype = 'Sales Associate' then 3
		when contacttype = 'Sales manager' then 4
	end as contacttypeid,

	cast(stateprovincename 		as varchar(20)) as statename,
	cast(countryregionname 		as varchar(15)) as country,
	cast(addressline1 			as varchar(40)) as supplieraddress,
	cast(coalesce(addressline2, 'Not Available') as varchar(20)) as adressline,
	cast(city 					as varchar(20)) as city,
	cast(postalcode 			as varchar(10)) as postalcode,
	cast(isactive 				as varchar(5)) as isactive,
	cast(averageleadtime 		as smallint) as leadtimeindays,
	cast(standardprice 			as numeric(9,2)) as prodocutprice,
	cast(lastreceiptcost 		as numeric(9,2)) as lastcost ,
	cast(lastreceiptdate 		as date) as lastreceiptdate,
	cast(minorderqty 			as int) as minimunorder,
	cast(maxorderqty 			as int) as maxorder,
	cast(coalesce(onorderqty, 0) as smallint) as onorderqty,

	case 
		when unitmeasurecode = 'ea' then 'Each'
		when unitmeasurecode = 'cs' then 'Case'
		when unitmeasurecode = 'pak' then 'Pack'
		when unitmeasurecode = 'dz' then 'Dozen'
		when unitmeasurecode = 'ctn' then 'Carton'
		when unitmeasurecode = 'gal' then 'Galoon'
		
	else 'Unknown'
	end as unitmeasure

from {{ ref ('dimprod_suppliername') }}
)

select * from elt_suppliername;