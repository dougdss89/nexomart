{% macro add_identity_columns() %}

    {% set identity_commands %}

        alter table [dw_stage].[dim_ddsalesreason] add salesreasonkey int not null identity (1, 1);

        alter table [dw_stage].[dim_ddtransactioncode] add transactioncodekey int not null identity (1, 1);

        alter table [dw_stage].[dim_ddunitmeasure] add unitmeasurekey int not null identity (1, 1);
        
        alter table [dw_stage].[dimbuyer] add buyerkey int not null identity (1, 1);

        alter table [dw_stage].[dimcountryregion] add countryregionkey int not null identity (1, 1);

        alter table [dw_stage].[dimcurrency_name] add currencyname_key int not null identity (1, 1);

        alter table [dw_stage].[dimcurrency] add currencykey int not null identity (1, 1);

        alter table [dw_stage].[dimcustomer] add customerkey int not null identity (1, 1);

        -- alter table [dw_stage].[dimdate] add datesurkey int not null identity (1, 1);

        alter table [dw_stage].[dimfreight] add freightkey int not null identity (1, 1);

        alter table [dw_stage].[dimproduct] add productkey int not null identity (1, 1);

        alter table [dw_stage].[dimproductcategory] add prodcategorykey int not null identity (1, 1);

        alter table [dw_stage].[dimpromotion] add promotionkey int not null identity (1, 1);

        alter table [dw_stage].[dimsalesperson] add salespersonkey int not null identity (1, 1);
        
        alter table [dw_stage].[dimscrapreason] add scrapeasonkey int not null identity (1, 1);

        alter table [dw_stage].[dimstateprovince] add stateprovincekey int not null identity (1, 1);

        alter table [dw_stage].[dimstore] add storekey int not null identity (1, 1);
        
        alter table [dw_stage].[dimsupplier_company] add supplierkey int not null identity (1, 1);

        alter table [dw_stage].[dimprod_suppliername] add supplierkey int not null identity (1, 1);

        alter table [dw_stage].[dimwarehouse] add warehousekey int not null identity (1, 1);

    {% endset %}

    {% do run_query(identity_commands) %}

{% endmacro %}