with limpa_nome as(

select 
	productid,
	replace(productname, ',', '-') as productname
from {{ ref ('dimproduct') }}
),

splita_coluna as (

  select
	productid,
	replace(productname, '/', '-') as productname,
	cast([value] as varchar(100)) as [value],
	ROW_NUMBER() over (partition by productid
						order by productid) as rn
from limpa_nome
cross apply (select
				*
			from string_split(productname, '-'))a),

modifica_value as (

	select
		productid,
		productname,
		case
			when trim([value]) like'%Blue' or [value] like '%Grey' or [value] like '%Multi' or [value] like '%Red' or [value] like'%Silver' then SUBSTRING([value],1,4)
			when trim([value]) like '%Silver/Black' or [value] like '%White'  or [value] like '%Yellow' then SUBSTRING([value],1,4)
			when trim([value]) like '[a-zA-Z]' then null
		else [value]
		end as tempvalue,
		rn
	from splita_coluna),

une_coluna_um as (

	
	select productid, tempvalue, rn
	from modifica_value
	where rn = 1),

une_coluna_dois as (

	select productid, 
	case 
		when tempvalue like '%black' then REPLACE(tempvalue, 'black', '')
		when tempvalue in ('yel', 'blu', 'sil', 'red') then ''
	else tempvalue
	end as tempvalue,
	rn
	from modifica_value
	where rn = 2),

join_colunas as (

	select 
		ucu.productid,
		rtrim(ltrim(coalesce(ucu.tempvalue,''))) as ucum, 
		case
			when udc.tempvalue like '%yel' or udc.tempvalue like  '%blu' or udc.tempvalue like '%sil' or udc.tempvalue like '%red' then ''
		else udc.tempvalue
		end as ucdois
	from une_coluna_um as ucu 
	left join
	une_coluna_dois as udc
	on ucu.ProductID = udc.ProductID),

join_hmlproduct as (

select
	stp.productid,
	(ucum +' '+ rtrim(ltrim(coalesce(ucdois, '')))) productname,
	productnumber,
	productsubcategoryid,
	subcategoryname,
	productcategoryid,
	categoryname,
	color,
	class,
	size,
	productline,
	productmodelid,
	modelname,
	style,
	listprice,
	standardcost,
	sellstartdate,
	sellenddate,
	safetystocklevel,
	reorderpoint,
	[weight],
	sizeunitmeasurecode,
	weightunitmeasurecode,
	daystomanufacture,
	discontinueddate,
	productkey
from join_colunas as jcol
	left join
	{{ ref('dimproduct') }} as stp
on jcol.productid = stp.ProductID),

elt_dim_product as (
	
select
	productkey,
	productid,
	productname,
	ProductNumber as productserial,
	coalesce(ProductSubcategoryID, 0) as subcategoryid,
	coalesce(subcategoryname, 'Discontinued') as subcategoryname,
	coalesce(ProductCategoryID, 0) as categoryid,
	coalesce(categoryname, 'Discontinued') as categoryname,

	case
		when ProductCategoryID is null and [subcategoryname] is null and ProductCategoryID is null and Color is null then 'Discontinued'
		when Color is null then 'Multicolor'
		when color = 'Multi' then 'Multicolor'
		when Color like '%/%' then 'Multicolor'
	else Color
	end as productcolor,

	case
		when ProductCategoryID is null and [subcategoryname] is null and ProductCategoryID is null and ProductLine is null then 'Discontinued'
		when ProductLine = 'R' then 'Road'
		when ProductLine = 'S' then 'Sport'
		when ProductLine = 'M' then 'Mountain'
		when ProductLine = 'T' then 'Touring'
	else 'Accessories'
	end as productline,

	case 
		when ProductCategoryID is null and [subcategoryname] is null and ProductCategoryID is null and Class is null then 'Discontinued'
		when Class = 'L' then 'Light'
		when Class = 'M' then 'Medium'
		when Class = 'H' then 'Heavy'
	else 'Small'
	end as productclass,

	case
		when Style = 'W' then 'Feminine'
		when Style = 'M' then 'Masculine'
		else 'Unissex'
	end as productstyle,

	case
		when ProductSubcategoryID is null and ProductCategoryID is null then 'Discontinued'
		when [modelname] like 'LL%' then REPLACE([modelname], 'LL', 'Light')
		when [modelname] like 'ML%' then REPLACE([modelname], 'ML', 'Medium')
		when [modelname] like 'HL%' then REPLACE([modelname], 'HL', 'Heavy')
	else [modelname]
	end as modelname,

	case	
		when ProductSubcategoryID is null and ProductCategoryID is null then 'Discontinued'
		when Size = 'L' then 'Large'
		when Size = 'M' then 'Medium'
		when Size = 'Xl' then 'Extra Large'
		when Size = 'S' then 'Small'
		when Size <= '42' then 'Small'
		when Size > '42' and Size <= '50' then 'Medium'
		when Size > '50' and Size <= '60' then 'Large'
		when Size > '60' then 'Extra Large'
	else 'Small Piece'
	end as productsize,

	case
		when ProductSubcategoryID is null and ProductCategoryID is null then 'Discontinued'
		when Size >= '38' and Size <= '70' then size + ' - Centimeters'
	else 'Not Available'
	end as sizeunit,

	case 
		when weightunitmeasurecode is null then 'Not Available'
		when weightunitmeasurecode = 'G' then 'Grams'
		when weightunitmeasurecode = 'LB' then 'Pounds'
	else weightunitmeasurecode
	end as weightcode,

	coalesce([weight], 0.00) as productweight,
	coalesce(sizeunitmeasurecode, 'Not Available') as measurecode,

	StandardCost as productcost,
	ListPrice as productprice,
	SafetyStockLevel as stocklevel,
	ReorderPoint as reorder,
	daystomanufacture,

	case 
		when DaysToManufacture <= '1' then 'Fast'
		when DaysToManufacture > '1' and DaysToManufacture <= '3' then 'Normal'
		when DaysToManufacture > '3' then 'Slowly'
	end as manufactureclass,

	cast(SellStartDate as date) as sellstartdate,

	case
		when ProductSubcategoryID is null and ProductCategoryID is null then 
                                                                        (select cast(max(sellstartdate) as date) from {{ ref('dimproduct') }} )
		when SellEndDate is not null then cast(SellEndDate as date)
	else '9999-12-31'
	end as sellenddate

from join_hmlproduct),

converte_dim_produto as (

select 

	cast(productkey as int) as productkey,
	cast(productid as smallint) as productid,
	cast(productname as nvarchar(50)) as productname,
	cast(productserial as nvarchar(20)) as productserial,
	cast(subcategoryid as smallint) as subcategoryid,
	cast(subcategoryname as varchar(15)) as subcategoryname,
	cast(categoryid as smallint) as categoryid,
	cast(categoryname as varchar(20)) as categoryname,
	cast(productcolor as varchar(15)) as productcolor,
	cast(productline as varchar(15)) as productline,
	cast(productclass as varchar(15)) as productclass,
	cast(productstyle as varchar(10)) as productstyle,
	cast(modelname as nvarchar(50)) as productmodel,
	cast(productsize as varchar(15)) as productsize,
	cast(sizeunit as nvarchar(20)) as sizeunit,
	cast(measurecode as varchar(5)) as measurecode,
	cast(productweight as numeric(9,2)) as productweight,
	cast(weightcode as varchar(15)) as weightcode,
	cast(productcost as numeric(12, 2)) as productcost,
	cast(productprice as numeric(12,2)) as productprice,
	cast(stocklevel as smallint) stocklevel,
	cast(reorder as smallint) as reorder,
	cast(daystomanufacture as varchar(10)) as daystomanufacture,
	cast(manufactureclass as varchar(10)) as manufactureclass,
	sellstartdate,
	sellenddate

from elt_dim_product),

dim_produto_final as (

select * from converte_dim_produto)

select * from dim_produto_final
where subcategoryid = 0 and categoryname  like 'Discontinued';