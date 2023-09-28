{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimwarehouse] 
				  		     add warehousekey int not null identity (1, 1)"}
    )
}}

select

	ppi.locationID,
	ppi.shelf,
	ppi.bin,
	ppi.quantity,
	ppi.productid,
	pl.[availability],
	pl.costrate,
	pl.[name],
	pbm.billofmaterialsid,
	pbm.componentid,
	pbm.startdate,
	pbm.bomlevel,
	pbm.enddate,
	pbm.perassemblyqty,
	pbm.unitmeasurecode

from {{ source ('production_src_schema', 'productinventory') }} as ppi
	left join
	{{ source ('production_src_schema', 'location') }} as pl
on ppi.locationid = pl.locationid
	left join
	{{ source ('production_src_schema', 'billofmaterials') }} as pbm
on ppi.productid = pbm.productassemblyid