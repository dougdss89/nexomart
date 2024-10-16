with elt_transactioncode as (

    select

        cast(coalesce(transactioncodekey, 0) as int) as transactioncodekey,
        cast(salesorderid as int) as salesorderid,
        cast(coalesce(purchaseordernumber, 'Not Available') as varchar(20)) as purchaseordernumber,
        cast(coalesce(carriertrackingnumber, 'Not available') as varchar(15)) as carriertracknumber,
        cast(accountnumber as varchar(15)) as accountnumber,
        cast(coalesce(creditcardapprovalcode, 'Other') as varchar(20)) as creditaprovacode,

        case
            when onlineorderflag = 1 then 'Yes'
            else 'No'
        end as isonlineorder
        
    from {{ref ('dim_DDtransactioncode') }}

)

select * from elt_transactioncode;