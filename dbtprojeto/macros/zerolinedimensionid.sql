{% macro zeroline_dimension() %}

    {% set zeroline_dimension %}

        insert into  [dw_stage].[buyer] values (0, 0, 'Not Available', 'Not Available', '9999-12-31', 999, 'Not Av', '9999-12-31', 0, 'Not Available', 'Not Available', 'Not Available', 0, 0.00, '9999-12-31', '9999-12-31', '9999-12-31', 0, 0);

        insert into  [dw_stage].[country] values (0, 0, 'Not Available', 'N/A', 'Not Available', 'N/A', 'Not Available');

        insert into  [dw_stage].[currency_name]values (0, 'N/A', 'Not Available', 'N/A', 'Not Available') set identity_insert into [dw_stage].currency_name ON;

        insert into  [dw_stage].[currency_rate] values (0, 0, 'N/A', 'N/A', 0.00, 0.00, '9999-12-31') set identity_insert into [dw_stage].currency_rate ON;

        insert into  [dw_stage].[customer] values(0, 0, 0, 'Not Available', 'Not Available', 'Not Available', 'Not Av.', 'Not Av.', '9999-12-31', 999, 'Not Available', 'Not Available', 0, 0, '0', '0', 0.00, 0, 'Not Available', 0, 'N/A', 0,'N/A', 'N/A', 'Not Available', 'Not Available', 'Not Av.', 'Not Available', 'Not Available', 'N/A');

        insert into  [dw_stage].[dd_salesreason] values (0, 0, 'Not Available', 'Not Available');

        insert into  [dw_stage].[dd_scrapreason] values (0, 0, 'Not Available');

        insert into  [dw_stage].[dd_transactioncode] values (0, 0, 'Not Available', 'Not Available', 'Not Available', 'N/A', 'N/A');

        insert into  [dw_stage].[ddate] values (000000, '9999-12-31', '9999-12-31', '9999-12-31', '9999-12-31', '9999-12-31', '9999-12-31', 0, 'Not Available', 'N/A', 0, 'NA', 'Not. Av.', 'Not Av', '9999-12-31', 'Not Available', 'N/A', 'Not.Av', 0, 'NA', 'NA', 'NA', 'NA');

        insert into  [dw_stage].[ddunitmeasure] values(0, 'N/A', 'Not Available');

        insert into  [dw_stage].[freight] values (0, 0, 'Not Available', 0.00, 0.00, 'Not. Av');

        insert into  [dw_stage].[prod_suppliername] values (0, 0, 'Not Available', 0, 'Not Available', 'Not Available', 'Not Available', 'Not Available', 0, 'Not Available', 'Not Available', 'Not Available', 'Not Available', 'Not. Av', 'N/A', 'N/A', 0, 0.00, 0.00, '9999-12-31', 0, 0, 0, 'N/A');

        insert into  [dw_stage].[product] values(0, 0, 'Not Available', 'Not Available', 0, 'Not Available', 0, 'Not Available', 'Not Available', 'Not Available', 'Not Available', 'Not Av.', 'Not Available', 'Not Available', 'Not Available', 0.00, 0.00, 'Not Available', 0.00, 0.00, 0, 0, 'Not. Av', 'Not. Av', '9999-12-31', '9999-12-31');

        insert into  [dw_stage].[product_discontinued] values(0, 0, 'Not Available', 'Not Available', 0, 'Not Available', 0, 'Not Available', 'Not Available', 'Not Available', 'Not Available', 'Not Av.', 'Not Available', 'Not Available', 'Not Available', 0.00, 0.00, 'Not Available', 0.00, 0.00, 0, 0, 'Not. Av', 'Not. Av', '9999-12-31', '9999-12-31');

        insert into  [dw_stage].[productcategory] values (0, 0, 'Not Available');

        insert into  [dw_stage].[promotion] values (0, 0, 'Not Available', 0.00, 'Not Available', 'Not Available', 0, 0, '9999-12-31', '9999-12-31', 'N/A');

        insert into  [dw_stage].[salesperson] values (0, 0, 'Not Available', 'Not Available', 'Not Available', 'Not Av.', 0, '9999-12-31', 0, 'Not Available', 'Not Av.', 'Not Available', 0, 'N/A', 'Not Available', 0.00, 0, '9999-12-31', 0.00, 0.00, 0.00, 'N/A', 'N/A');;

        insert into  [dw_stage].[stateprovince] values (0, 0, 'N/A', 0, 'N/A', 'Not Available');

        insert into  [dw_stage].[store] values (0, 0, 0, 0, 'Not Available', 'Not Available', 0, 0, 0, 'Not Available', 'Not Av.', 'Not Available', 0.00, 'Not Available', 0, 0, 'Not Available', 'Not Available', 'Not. Av');

        insert into  [dw_stage].[store_archive] values (0, 0, 0, 0, 'Not Available', 'Not Available', 0, 0, 0, 'Not Available', 'Not Av.', 'Not Available', 0.00, 'Not Available', 0, 0, 'Not Available', 'Not Available', 'Not. Av');

        insert into  [dw_stage].[supplier_company] values (0, 0, 'Not Available', 'Not Available', 0, 'Not Available', 'N/A', 'Not Available', 'Not Available', 'Not Available', 'Not Available', 'Not Available', 'Not Av.', 'N/A');

        insert into  [dw_stage].[warehouse] values (0, 0, 'Not Av.', 0, 0, 0, 0.00, 0.00, 'Not Available', 0, 0, 0, 0, 'Not Available', '9999-12-31', '9999-12-31');

    {% endset %}

    {% do run_query(zeroline_dimension) %}

{% endmacro %}
