	select 
    
        datekey as shipdatekey,
        orderdate as shipmentdate,
        *

    from {{ ref('dimdate') }}
