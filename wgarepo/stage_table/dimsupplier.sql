{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimsupplier] 
				  		     add supplierkey int not null identity (1, 1)"}
    )
}}

select

	ppv.productid,
	pv.businessentityid,
	accountnumber,
	pv.name,
	pvc.contacttype,
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
	pva.postalcode

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
