
--seleciona as colunas para a dimensão frete
with elt_freight as (
select 

	freightkey,
    shipmethodid as shipid,
    [name] as freightname,
    shipbase,
    shiprate,

	case 
		when shipbase < 5.00 then 'Basic'
		when shipbase > 5.00 and shipbase < 10.00 then 'Normal'
		when shipbase > 10.00 and shipbase < 25.00 then 'Express'
		else 'Deluxe'
	end as freightclass
    
from {{ ref('dimfreight') }}
),

normaliza_datatype as (

    select 
        cast(freightkey as int) as freightkey,
        cast(shipid as int) as shipid,
        cast(freightname as varchar(20)) as freightname,
        cast(shipbase as numeric(12, 2)) as shipbase,
        cast(shiprate as numeric(5, 2)) as shiprate,
        cast(freightclass as varchar(10)) as freightclass

    from elt_freight
)

-- select final

select * from normaliza_datatype;