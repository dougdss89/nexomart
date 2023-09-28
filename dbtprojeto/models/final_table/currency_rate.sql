select

    currencykey,
    currencyrateid,
    fromcurrencycode,
    tocurrencycode,
    averagerate,
    endofdayrate,
    currencyratedate

from {{ ref ('dimcurrency') }}