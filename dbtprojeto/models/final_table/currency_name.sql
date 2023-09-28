select 

    currencyname_key,
    currencycode,
    currencyname,
    countryregioncode as countrycode,
    countryname

from {{ ref ('dimcurrency_name') }}