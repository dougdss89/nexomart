
select

	ppv.productid,
	pv.businessentityid,
	accountnumber,
	pv.name,
	pvc.contacttype,

	case
		when contacttype = 'Assistant Sales Agent' then 1
		when contacttype = 'Sales Agent' then 2
		when contacttype = 'Sales Associate' then 3
		when contacttype = 'Sales manager' then 4
	end as contacttypeid,
		
	pvc.firstname,
	pvc.lastname,
	pvc.phonenumber,
	pvc.phonenumbertype,
	creditrating,
	preferredvendorstatus,
	averageleadtime,
	standardprice,
	lastreceiptcost,
	lastreceiptdate,
	minorderqty,
	maxorderqty,
	onorderqty,
	unitmeasurecode,
	pva.stateprovincename,
	pva.countryregionname,
	pva.addressline1,
	pva.addressline2,
	pva.city,
	pva.postalcode,
	pv.activeflag

from {{ source ('purchasing_src_schema', 'Vendor' ) }} as pv
	left join
	{{ source ('purchasing_src_schema', 'productvendor' ) }} as ppv
on pv.businessentityid = ppv.businessentityid
	left join
	{{ source ('purchasing_src_schema', 'vVendorWithAddresses' ) }} as pva
on pv.businessentityid = pva.businessentityid
	left join
	{{ source ('purchasing_src_schema', 'vVendorWithContacts' ) }} as pvc
on pva.businessentityid = pvc.businessentityid
where activeflag = 1 and productid is not null;
