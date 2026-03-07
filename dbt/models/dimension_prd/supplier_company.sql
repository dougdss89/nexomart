with elt_supplier as (

select 
    supplierkey,
	businessentityid,
	accountnumber,
	name as supplliername,
	

	case 
		when creditrating = 1 then 'Premier'
		when creditrating between 2 and 3 then 'Super class'
		when creditrating = 4 then 'Normal'
		when creditrating = 5 then 'Attention'
	else 'No Classification'
	end as ratingclass,

	creditrating,

	case
		when preferredvendorstatus = 1 then 'Yes'
		else  'No'
	end as ispreferred,

	stateprovincename,
	countryregionname,
	addressline1,
	coalesce(addressline2, 'Not Available') as addressline2,
	city,
	postalcode,
	activeflag

from {{ref ('dimsupplier_company') }}
),

normaliza_datatype as (

    select

        cast(coalesce(supplierkey, 0) as int) as supplierkey,
        cast(businessentityid as int) as supplierid,
        cast(accountnumber as varchar(15)) as supplieraccount,
        cast(supplliername as varchar(40)) as supplliername,
        cast(creditrating as smallint) as creditrating,
        cast(ratingclass as varchar(15)) as ratingclass,
        cast(ispreferred as char(5)) as ispreferred,
		cast(stateprovincename 	as varchar(20)) as statename,
		cast(countryregionname 	as varchar(15)) as country,
		cast(addressline1 		as varchar(40)) as supplieraddress,
		cast(addressline2 		as varchar(20)) as addressline,
		cast(city 				as varchar(20)) as city,
		cast(postalcode 		as varchar(10)) as postalcode,
		cast(activeflag 		as varchar(5)) as isactivte

    from elt_supplier
)

select * from normaliza_datatype;