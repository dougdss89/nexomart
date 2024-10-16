select 

    coalesce(currencyname_key, 0) as currencyname_key,
    currencycode,
    currencyname,
    countryregioncode as countrycode,
    countryname

from {{ ref ('dimcurrency_name') }}