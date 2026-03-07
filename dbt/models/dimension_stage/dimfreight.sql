

select 

    coalesce(shipmethodid, 0) as shipmethodid,
    [Name],
    shipbase,
    shiprate

from {{ source ('purchasing_src_schema', 'shipmethod') }};
