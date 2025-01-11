select 

	pv.businessentityid,
	pv.accountnumber,
	pv.name,
	creditrating,
	preferredvendorstatus,
	pva.stateprovincename,
	pva.countryregionname,
	pva.addressline1,
	pva.addressline2,
	pva.city,
	pva.postalcode,
	pv.activeflag
	
from  {{ source ('purchasing_src_schema', 'Vendor' ) }} as pv

	left join

	{{ source ('purchasing_src_schema', 'vVendorWithAddresses' ) }} as pva

on pv.businessentityid = pva.businessentityid
