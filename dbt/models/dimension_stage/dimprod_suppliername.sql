-- dimensão product_suplliername
select 

	ppv.ProductID, 
	pv.AccountNumber,
	pv.BusinessEntityID as supplierid,
	pv.[name] as companyname,
	pvc.firstname,
	pvc.lastname,
	contacttype,

	case
		when contacttype = 'Assistant Sales Agent' then 1
		when contacttype = 'Sales Agent' then 2
		when contacttype = 'Sales Associate' then 3
		when contacttype = 'Sales manager' then 4
	end as contacttypeid,

	averageleadtime,
	standardprice,
	lastreceiptcost,
	lastreceiptdate,
	minorderqty,
	maxorderqty,
	onorderqty,
	unitmeasurecode,
	
	case
		when activeflag = 1 then 'Yes'
		else 'No'
	end as isactive,

	pva.AddressType,
	pva.CountryRegionName,
	pva.stateprovincename,
	pva.city,
	pva.postalcode,
	pva.addressline1,
	pva.addressline2

from {{ source ('purchasing_src_schema', 'Vendor' ) }} as pv
	left join
	{{ source ('purchasing_src_schema', 'productvendor' ) }} as ppv
on pv.businessentityid = ppv.businessentityid
	left join
	{{ source ('purchasing_src_schema', 'vVendorWithAddresses' ) }} as pva
on pv.businessentityid = pva.businessentityid
	left join
	{{ source ('purchasing_src_schema', 'vVendorWithContacts' ) }} as pvc
on pva.businessentityid = pvc.businessentityid;