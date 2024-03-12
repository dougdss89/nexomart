{% macro add_fctidentity_columns() %}

    {% set identity_commands %}

        alter table [dw_fact].[fact_production] add factproductionkey int not null identity (1, 1);

        alter table [dw_fact].[fact_purchase] add factpurchasekey int not null identity (1, 1);

        alter table [dw_fact].[fact_sales] add factsaleskey int not null identity (1, 1);
        
    {% endset %}

    {% do run_query(identity_commands) %}

{% endmacro %}



-- {% macro adicionar_coluna_identidade(model, column_name) %}

--     {% set sql %}

--         alter table {{ model }} add {{ column_name }} int not null identity(1 , 1);

--     {% endset %}

--     {% do run_query(sql) %}

-- {% endmacro %}
