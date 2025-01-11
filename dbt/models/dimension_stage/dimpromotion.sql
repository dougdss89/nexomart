
select 

    coalesce(specialofferid, 0) as specialofferid,
    [description],
    discountpct,
    [type],
    category,
    minqty,
    maxqty,
    startdate,
    enddate

from {{ source ('salestables_src_schema', 'specialoffer') }};
