with elt_category as (

    select

        prodcategorykey,
        productcategoryid,
        categoryname

    from {{ref ('dimproductcategory') }}

),

normalize_table as(

    select

        cast(prodcategorykey as smallint) as categorykey,
        cast(productcategoryid as smallint) as categoryid,
        cast(categoryname as varchar(15)) as categoryname

    from elt_category

)

select * from normalize_table;