{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimproduct] 
				  		     add productkey int not null identity (1, 1)"}
    )
}}


select

    pp.ProductID,
    pp.name as productname,
    pp.productnumber,
    pps.productsubcategoryid,
    pps.name subcategoryname,
    ppc.productcategoryid,
    ppc.name as categoryname,
    pp.color,
    pp.class,
    pp.size,
    pp.productline,
    pp.productmodelid,
    ppm.name as modelname,
    pp.style,
    pp.listprice,
    pp.standardcost,
    sellstartdate,
    pp.sellenddate,
    pp.safetystocklevel,
    pp.reorderpoint,
    pp.sizeunitmeasurecode,
    pp.weightunitmeasurecode,
    pp.[weight],
    pp.daystomanufacture,
    pp.discontinueddate

from {{source ('production_src_schema', 'product')}} as pp
left join
    {{source ('production_src_schema', 'productsubcategory')}} as pps
on pp.productsubcategoryid = pps.productsubcategoryid
left join
    {{source ('production_src_schema', 'productcategory')}} as ppc
on pps.productcategoryid = ppc.productcategoryid
left join
    {{source ('production_src_schema', 'productmodel')}} as ppm
on pp.productmodelid = ppm.productmodelid