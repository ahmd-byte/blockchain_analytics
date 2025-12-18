{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        This macro controls how dbt generates schema names.
        
        In production, we want to use the custom schema names directly
        (e.g., 'staging', 'analytics') without the target prefix.
        
        In development, we prefix with the target schema for isolation.
    #}
    
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {# In production, use the custom schema name directly #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {# In dev/ci, prefix with target schema for isolation #}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
    
{%- endmacro %}

