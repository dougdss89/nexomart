{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimcustomer] 
				  		     add customerkey int not null identity (1, 1)"}
    )
}}

select
	pp.businessentityid,
	sc.customerid,
	pp.firstname,
	pp.lastname,
	pp.persontype,
	pp.emailpromotion,
	ppt.[name] as typephone,
	pph.phonenumber,
	pad.postalcode,
	pat.name as addresstype,
	emailaddress,
	psp.territoryid,
	pcr.[name] as countryname,
	pad.stateprovinceid,
	psp.countryregioncode,
	psp.stateprovincecode,
	psp.[name] as statename,
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
left join 
	{{ source ('person_src_schema', 'person') }} as pp
on sc.personid = pp.businessentityid
left join
	{{ source ('person_src_schema', 'personphone') }} as pph
on pp.businessentityid = pph.businessentityid
left join
	{{ source ('person_src_schema', 'phonenumbertype') }} as ppt
on pph.phonenumbertypeid = ppT.phonenumbertypeid
left join
	{{ source ('person_src_schema', 'businessentityaddress') }} as pba
on pp.businessentityid = pba.businessentityid
left join
	{{ source ('person_src_schema', 'address') }} as pad
on pba.addressid = pad.addressid
left join
	{{ source ('person_src_schema', 'addresstype') }} as pat
on pba.addresstypeid = pat.addresstypeid
left join
	{{ source ('person_src_schema', 'emailaddress') }} as pea
on pp.businessentityid = pea.businessentityid
left join
	{{ source ('person_src_schema', 'stateprovince') }} as psp
on pad.stateprovinceid = psp.stateprovinceid
left join
	{{ source ('person_src_schema', 'countryregion') }} as pcr
on psp.countryregioncode = pcr.countryregioncode
where pp.businessentityid not in (select businessentityid
									from {{ source ('humanres_src_schema', 'employee') }})