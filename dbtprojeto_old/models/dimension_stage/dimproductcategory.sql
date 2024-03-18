

select 

	coalesce(ProductCategoryID, 0) as productcategoryid,
	[name] as categoryname

from {{source ('production_src_schema', 'productcategory')}}
