
with store_elt as (

	select
        storekey,
		storeid,
		personid as managerid,
		customerid,
		storename,
		firstname +' ' + LastName as managername ,
		salespersonid as salesrepresentant,
		yearopened,
		datediff(y, YearOpened, cast(year(getdate()) as int)) as yearsactive,

		case 
			when Specialty = 'OS' then 'Online Store'
			when Specialty = 'BM' then 'Bike Market'
			else 'Bike Store'
		end as businesstype,
		
		specialty,
		
		case 
			when brands = '4+' then 'Four or More'
			when brands = '2' then 'Two Brands'
			when brands = '1' then 'One Brand'
			when brands = '3' then 'Three brands'
			else 'Adv Works'
		end as brands,

		internet,

		case
			when AnnualSales > 0 then AnnualRevenue
			else AnnualSales
		end as annualsales,

		annualrevenue,

		case
			when annualrevenue <= 30000.00 then 'Local store'
			when annualrevenue > 30000.00 and annualrevenue < 80000.00 then 'Small store'
			when annualrevenue > 80000.00 and annualrevenue < 100000.00 then 'Medium store'
			when annualrevenue > 100000 and annualrevenue < 150000.00 then 'Large store'
			else 'Global company'
		end as storesize,

		numberemployees,
		territoryid,
		continentname,
		regionname,

		case
			when regionname not like 'United States' then 'Central'
		else CountryRegionCode
		end as countryregion

from {{ ref('dimstore') }}
),

normaliza_store_datatype as (

    select

        cast(storekey as int) as storekey,
        cast(storeid as int) as storeid,
        cast(managerid as int) as managerid,
        cast(customerid as int) as customerid,
        cast(storename as varchar(50)) as storename,
        cast(managername as varchar(50)) as managername,
        cast(salesrepresentant as int) as salesrepresentant,
        cast(yearopened as int) as yearopened,
        cast(yearsactive as int) as yearsactive,
        cast(businesstype as varchar(20)) as businesstype,
        cast(specialty as varchar(10)) as speciality,
        cast(brands as varchar(15 )) as brands,
        cast(annualrevenue as decimal(12,2)) as annualrevenue,
        cast(storesize as varchar(15)) as storesize,
        cast(numberemployees as smallint) as numberemployees,
        cast(territoryid as smallint) as territoryid,
        cast(continentname as varchar(20)) as continentname,
        cast(regionname as varchar(15)) as regionname,
        cast(countryregion as varchar(10)) as countryregion
    
    from store_elt
)
select * from normaliza_store_datatype
where managerid is not null;