{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimproductcategory] 
				  		     add prodcategorykey int not null identity (1, 1)"}
    )
}}

select 

	coalesce(ProductCategoryID, 0) as productcategoryid,
	[name] as categoryname

from {{source ('production_src_schema', 'productcategory')}}
