with elt_transactioncode as (

    select

        cast(transactioncodekey as int) as transactioncodekey,
        cast(salesorderid as int) as salesorderid,
        cast(coalesce(purchaseordernumber, 'Not Available') as varchar(20)) as purchaseordernumber,
        cast(coalesce(carriertrackingnumber, 'Not available') as varchar(15)) as carriertracknumber,
        cast(accountnumber as varchar(15)) as accountnumber,
        cast(coalesce(creditcardapprovalcode, 'Other') as varchar(20)) as creditaprovacode,
        cast(onlineorderflag as char(2)) as isonlineorder

    from {{ref ('dim_DDtransactioncode') }}

)

select * from elt_transactioncode;