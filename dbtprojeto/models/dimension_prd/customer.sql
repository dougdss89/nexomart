with pre_customer as (
	
select
    businessentityid,
	customerid,
    firstname,
    lastname,
    persontype,
    emailpromotion,
    typephone,
    postalcode,
    addresstype,
    emailaddress,
    territoryid,
    countryname,
    stateprovinceid,
    countryregioncode,
    stateprovincecode,
    statename,
    city,
    addressline1,
    addressline2,
    totalpurchaseytd,
    datefirstpurchase,
    birthdate,
    maritalstatus,
    yearlyincome,
    gender,
    totalchildren,
    numberchildrenathome,
    education,
    occupation,
    homeownerflag,
    numbercarsowned,
    customerkey

-- referenciando o modelo da tabela stage customer
from {{ ref ('dimcustomer') }} 
 ),

/*
aqui começa a transformação da tabela, limpando os dados
removendo nulls
*/
elt_clientes as (

select
		customerkey,
		businessentityid as personid,
		customerid,
		firstname, 
		lastname,
		persontype,

		case
			when gender = 'F' then 'Female'
			when gender = 'M' then 'Male'
			when gender IS NULL then 'Not Available'
		end as gender,

		case
			when maritalstatus = 'M' then 'Married'
			when maritalstatus = 'S' then 'Single'
			when maritalstatus IS NULL then 'Not Available'
		end as maritalstatus,
		coalesce(cast(birthdate as date), '9999-12-31') as birthdate,
		coalesce(datediff(yy, BirthDate, GETDATE()), 999) as age,
		coalesce(education, 'Not Available') as education,
		coalesce(occupation, 'Not Available') as occupation,
		coalesce(numberchildrenathome, 0) as numchildreninhome,
		coalesce(totalchildren, 0) as totalchildren,
		coalesce(trim(substring(yearlyincome, 0, CHARINDEX('-', yearlyincome))), '0') as minincome,
		coalesce(trim(substring(yearlyincome, CHARINDEX('-', yearlyincome)+1, 20)), '0') as maxincome,
		cast(totalpurchaseytd as numeric(9,2)) as totalpuchased,

		case
			when HOMEOWNERFLAG = 1 then 'Yes'
			else 'No'
		end as ishomeowner,
		coalesce(NUMBERCARSOWNED, 0 ) as numberscarsowned,

		case
			when emailpromotion >= 1 then 'Yes'
			else 'No'
		end as emailpromotion,
		emailaddress,

		case
			when typephone = 'cell' then 'Mobile'
			else coalesce(typephone, 'Not Available')
		end as typephone,
		coalesce([addresstype], 'Not Available') as typeaddress,
		coalesce(postalcode, 'Not Available') as postalcode,
		coalesce(territoryid, 0) as territoryid,
		coalesce(countryname, 'Not Available') as countryname,
		coalesce(stateprovinceid, 0) as stateprovinceid,
		coalesce(countryregioncode, 'Not Available') as countryregioncode,
		coalesce(stateprovincecode, 'Not Available') as stateprovincecode,
		coalesce(statename, 'Not Available') as statename,
		coalesce(city, 'Not Available') as city,
		coalesce(addressline1, 'Not Available') as custaddress,
		coalesce(addressline2, 'Not Available') as addressline,
		ROW_NUMBER() over(partition by businessentityid
					order by businessentityid) as rn,

	case 
		when [value] = 'DR.' or [value] = 'Dr' then 'Drive'
		when [value] = 'ST.' or [value] = 'St' or [value] = 'Str' then 'Street'
		when [value] = 'Ave.' or [value] = 'Ave' then 'Avenue'
		when [value] = 'Ln.' or [value] = 'LN' then 'Lane'
		when [value] = 'Ct.' or [value] = 'Ct' then 'Court'
		when [value] = 'Rd.' or [value] = 'RD' then 'Road'
		when [value] = 'Pkwy.' or [value] = 'pkwy' then 'Parkway'
		when [value] = 'pl.' or [value] = 'pl' then 'Plaza'
		when [value] = 'Mt.' or [value] = 'MT' then 'Mount'
		when [value] = 'Blvd.' or [value] = 'Blvd' then 'Boulevard'
		when [value] = 'Rt.' or [value] = 'Rt' then 'Route'
		when [value] = 'Pl.' or [value] = 'Pl' then 'Plaza'
		when [value] = 'N.' then 'North'
	else [value]
	end as tempaddress
from pre_customer

/*
as próximas 4 cte's criadas são utilizadas para ajustar e limpar o endereço
como não existia um padrão, precisei separar em linhas com string split
separado com string_split e window function, une utilizando  as cte's
*/

cross apply (select
			*
			from string_split(AddressLine1, ' '))as split),

une_col_um as (

select 
    personid, 
    tempaddress as line_um from elt_clientes
where rn = 1),

une_col_dois as (

select personid, tempaddress as line_dois
from elt_clientes
where rn = 2),

une_col_tres as (

select 
    personid, 
    tempaddress as line_tres 
from elt_clientes
where rn = 3),

une_col_quatro as(
select 
    personid, 
    tempaddress as line_quatro from elt_clientes
where rn =  4),

une_endereco as (

-- aqui acaba a separação, concatenando as colunas desejadas
-- agora, há uma única coluna com endereço, com os valores no padrão: endereço + numero.
select 
	col1.personid, 
	(line_um +' '+ line_dois +' '+ line_tres +' '+ line_quatro) as endr
from une_col_um as col1
left join
	une_col_dois as col2
on col1.personid = col2.personid
left join
	une_col_tres as col3
on col1.personid = col3.personid
left join
	une_col_quatro as col4
on col1.personid = col4.personid),

-- finalizando o elt, atribuindo os devidos datatypes
-- unindo a coluna de endereço e selecionando o resultado final.

finaliza_etl as (

select distinct
	customerkey,
	cast(eltc.personid as int) as personid,
	cast(eltc.customerid as int) as customerid,
	cast(firstname as varchar(40)) as firstname,
	cast(lastname as varchar(40)) as lastname,
	cast(gender as varchar(10)) as gender,
	cast(maritalstatus as varchar(10)) as maritalstatus,
	cast(birthdate as date) as birthdate,
	cast(age as smallint) as age,
	cast(education as varchar(20)) as education,
	cast(occupation as varchar(20)) as occupation,
	cast(numchildreninhome as tinyint) as numchildreninhome,
	cast(totalchildren as tinyint) as totalchildren,
	case
		when maxincome like 'grea%' then '100000'
	else minincome
	end as minincome,
	cast(maxincome as varchar(20)) as maxincome,
	cast(totalpuchased as numeric(12,2)) as totalpurchased,
	cast(numberscarsowned as tinyint) as numbersofcarsowned,
	cast(emailaddress as varchar(100)) as emailaddress,
	cast(typephone as varchar(6)) as typephone,
	cast(typeaddress as varchar(6)) as typeaddress,
	cast(territoryid as smallint) as countryid,
	cast(countryname as varchar(15)) as countryname,
	cast(stateprovinceid as smallint) as stateid,
	cast(countryregioncode as char(5)) as countrycode,
	cast(stateprovincecode as char(5)) as statecode,
	cast(statename as varchar(25)) as statename,
	cast(city as varchar(20)) as city,
	cast(postalcode as nvarchar(10)) as postalcode,
	coalesce(cast(uned.endr as varchar(80)), 'Not Available') as custaddress,
	cast(addressline as varchar(15)) as addressline,
	cast(ishomeowner as char(5)) as ishomeowner,
	cast(emailpromotion as char(5)) as emailpromotion

from elt_clientes as eltc
	inner join
	une_endereco as uned
on eltc.personid = uned.personid)

select * from finaliza_etl;