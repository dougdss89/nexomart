
--criando a dimensão salesperson, tratando os dados da tabela.

with salesperson_etl as (

select 
	salespersonkey,
	BusinessEntityID as salespersonid,
	LoginID,
	firstname,
	lastname,
	case
		when Gender = 'M' then 'Male'
		when Gender = 'F' then 'Female'
	end as Gender,
	(DATEDIFF(YY, BirthDate, GETDATE())) as age,
	HireDate,
	(DATEDIFF(YY, HireDate, GETDATE())) as yearsincompany,
	GroupName as division,
	departmentname as subdivision,
	jobtitle,

	case
		when TerritoryID is null and CountryRegionCode is null and jobtitle like 'North American%' then 1
		when TerritoryID is null and CountryRegionCode is null and jobtitle like 'Pacific sales%' then  9
		when TerritoryID is null and CountryRegionCode is null and jobtitle like 'European Sales%' then 8
		else TerritoryID
	end as territoryid,

	case
		when TerritoryID is null and JobTitle like 'North American%' then 'US'
		when TerritoryID is null and JobTitle like 'European Sales%' then 'EU'
		when TerritoryID is null and JobTitle like 'Pacific Sales%' then 'UA'
	else CountryRegionCode
	end as countrycode,

	case
		when JobTitle like 'North american%' and continentname is null then 'USA HQ'
		when JobTitle like 'Pacific Sales%' and continentname is null then 'Sidney HQ'
		when JobTitle like 'European Sales%' and continentname is null then 'Berlim HQ'
	else continentname
	end as salesregion,

	cast(Rate as numeric(5,2)) as salaryhour,
	PayFrequency,
	cast(RateChangeDate as date) as salarychangedt,

	case 
		when JobTitle like 'North American%' and CommissionPct = 0.00 then  0.05
		when JobTitle like 'Pacific Sales%' and CommissionPct = 0.00 then 0.04
		when JobTitle like 'European Sales%' and CommissionPct = 0.00 then 0.04
	else commissionpct
	end as commission,

	case
		when JobTitle like 'North American%' and salesquota is null then cast((select max(SalesQuota) *.70 + max(salesquota) 
																					from {{ ref('dimsalesperson') }}) as numeric(9,2))
		when JobTitle like 'European Sales%' and SalesQuota is null then cast((select max(salesquota) *.65 + max(salesquota) 
																					from {{ ref('dimsalesperson') }}) as numeric(9,2))
		when JobTitle like 'Pacific Sales%' and SalesQuota is null then cast(( select max(salesquota) *.55 + max(SalesQuota) 
																					from {{ ref('dimsalesperson') }}) as numeric(9,2))
	else SalesQuota
	end as quota,

	bonus,

	case
		when SalariedFlag = 1 then 'Yes'
		else 'No'
	end as issalaried,

	case 
		when CurrentFlag = 1 then 'Yes'
		else 'No'
	end as isactive
from {{ ref('dimsalesperson') }}
),

-- normaliza os datatypes das colunas
-- ajusta a posição do manager na cláusula CASE.
salesperson_bonus as (

	select
		cast(salespersonkey as int) as  salespersonkey,
		cast(salespersonid as smallint) as salespersonid,
		cast(loginid as nvarchar(40)) as loginid,
		cast(firstname as varchar(20)) as firstname,
		cast(lastname as varchar(40)) as lastname,
		cast(gender as varchar(10)) as gender,
		cast(age as tinyint) as age,
		cast(hiredate as date) as hiredate,
		cast(yearsincompany as smallint) as yearsincompany,
		cast(division as varchar(50)) as division,
		cast(subdivision as varchar(10)) as subdivision,
		cast(jobtitle as varchar(40)) jobtitle,
		cast(territoryid as tinyint) as territoryid,
		cast(countrycode as varchar(5)) as countrycode,
		cast(salesregion as varchar(20)) as salesregion,
		cast(salaryhour as numeric(5,2)) as salaryhour,
		cast(payfrequency as tinyint) as payfrequency,
		cast(salarychangedt as date) as salarychangedt,
		cast(commission as numeric(5,3)) as commission,
		cast(quota as numeric(12,2)) as quota,

		case
			when JobTitle like '%Manager' then cast((quota * commission) as numeric(12,2))
			when Bonus < 100.00 then (Bonus * 10)
		else bonus
		end as bonus,

		cast(issalaried as char(5)) as issalaried,
		cast(isactive as char(5)) as isactive
	from salesperson_etl),

salesperson_final as (

select
		cast(salespersonkey as int) as  salespersonkey,
		cast(salespersonid as smallint) as salespersonid,
		cast(loginid as nvarchar(40)) as loginid,
		cast(firstname as varchar(20)) as firstname,
		cast(lastname as varchar(40)) as lastname,
		cast(gender as varchar(10)) as gender,
		cast(age as tinyint) as age,
		cast(hiredate as date) as hiredate,
		cast(yearsincompany as smallint) as yearsincompany,
		cast(division as varchar(50)) as division,
		cast(subdivision as varchar(10)) as subdivision,
		cast(jobtitle as varchar(40)) jobtitle,
		cast(territoryid as tinyint) as territoryid,
		cast(countrycode as varchar(5)) as countrycode,
		cast(salesregion as varchar(20)) as salesregion,
		cast(salaryhour as numeric(5,2)) as salaryhour,
		cast(payfrequency as tinyint) as payfrequency,
		cast(salarychangedt as date) as salarychangedt,
		cast(commission as numeric(5,3)) as commission,
		cast(quota as numeric(12,2)) as quota,
		cast(bonus as numeric(12,2)) as bonus,
		cast(issalaried as char(5)) as issalaried,
		cast(isactive as char(5)) as isactive
        
from salesperson_bonus)

select * from salesperson_final;
