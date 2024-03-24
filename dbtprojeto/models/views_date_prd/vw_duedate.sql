---- cria as roleplays views

	select 
        datekey as duedatekey,
        orderdate as duedate,
        *
    
    from {{ ref ('dimdate') }};