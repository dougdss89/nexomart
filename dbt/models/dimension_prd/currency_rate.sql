select

    coalesce(currencykey, 0) as currencykey,
    currencyrateid,
    fromcurrencycode,
    tocurrencycode,
    averagerate,
    endofdayrate,
    currencyratedate

from {{ ref ('dimcurrency') }}