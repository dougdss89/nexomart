
select
	
	sc.CustomerID,
	sc.PersonID,
	pp.firstname,
	pp.lastname,

	case
		when pp.persontype = 'in' then 'Individual'
		else 'Not Available'
	end as customertype,

	pad.postalcode,
	pat.name,
	emailaddress as email,
	psp.territoryid,
	pcr.name AS countryname,
	pad.stateprovinceid AS stateid,
	psp.countryregioncode AS countrycode,
	psp.stateprovincecode AS statecode,
	psp.name AS statename,
	pad.city,
	pad.addressline1,
	pad.addressline2,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:TotalPurchaseYTD)[1]','numeric(12,2)') AS TotalPurchaseYTD,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:DateFirstPurchase)[1]','date') AS DateFirstPurchase,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:BirthDate)[1]','date') AS BirthDate,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:MaritalStatus)[1]','varchar(10)') AS MaritalStatus,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:YearlyIncome)[1]','varchar(50)') AS YearlyIncome,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:Gender)[1]','varchar(10)') AS Gender,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:TotalChildren)[1]','smallint') AS TotalChildren,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:NumberChildrenAtHome)[1]','smallint') AS NumberChildrenAtHome,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:Education)[1]','varchar(50)') AS Education,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:Occupation)[1]','varchar(50)') AS Occupation,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:HomeOwnerFlag)[1]','varchar(5)') AS HomeOwnerFlag,
	pp.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; (/ns:IndividualSurvey/ns:NumberCarsOwned)[1]','smallint') AS NumberCarsOwned

from {{ source ('salestables_src_schema', 'customer') }} as sc
inner join 
	{{ source ('person_src_schema', 'person') }} as pp
on sc.personid = pp.businessentityid
inner join
	{{ source ('person_src_schema', 'personphone') }} as pph
on sc.personid = pph.businessentityid
inner join
	{{ source ('person_src_schema', 'phonenumbertype') }} as ppt
on pph.phonenumbertypeid = ppT.phonenumbertypeid
inner join
	{{ source ('person_src_schema', 'businessentityaddress') }} as pba
on sc.personid = pba.businessentityid
inner join
	{{ source ('person_src_schema', 'address') }} as pad
on pba.addressid = pad.addressid
inner join
	{{ source ('person_src_schema', 'addresstype') }} as pat
on pba.addresstypeid = pat.addresstypeid
inner join
	{{ source ('person_src_schema', 'emailaddress') }} as pea
on sc.personid = pea.businessentityid
inner join
	{{ source ('person_src_schema', 'stateprovince') }} as psp
on pad.stateprovinceid = psp.stateprovinceid
inner join
	{{ source ('person_src_schema', 'countryregion') }} as pcr
on psp.countryregioncode = pcr.countryregioncode;