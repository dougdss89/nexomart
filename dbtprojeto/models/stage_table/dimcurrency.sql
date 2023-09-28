{{ 
    config(

        post_hook = {"sql": "alter table [db_stage].[dimcurrency] 
				  		     add currencykey int not null identity (1, 1)"}
    )
}}

select 

    coalesce(scr.currencyrateid,0) as currencyrateid,
    scr.fromcurrencycode,
    scr.tocurrencycode,
    cast(scr.averagerate as money) as averagerate,
    cast(scr.endofdayrate as money) as endofdayrate,
    cast(scr.currencyratedate as date) as currencyratedate
    
from {{source ('currencyname_src_schemasales', 'currencyrate')}} as scr