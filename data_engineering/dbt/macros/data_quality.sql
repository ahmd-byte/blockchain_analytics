{% macro test_valid_ethereum_address(model, column_name) %}
    {#
        Test that a column contains valid Ethereum addresses.
        Valid addresses are 42 characters starting with '0x' followed by hex characters.
    #}
    
    select
        {{ column_name }} as invalid_address
    from {{ model }}
    where {{ column_name }} is not null
        and (
            length({{ column_name }}) != 42
            or left({{ column_name }}, 2) != '0x'
            or not regexp_contains({{ column_name }}, r'^0x[a-f0-9]{40}$')
        )
    
{% endmacro %}


{% macro test_positive_value(model, column_name) %}
    {#
        Test that a numeric column contains only non-negative values.
    #}
    
    select
        {{ column_name }} as negative_value
    from {{ model }}
    where {{ column_name }} is not null
        and {{ column_name }} < 0
    
{% endmacro %}


{% macro test_timestamp_not_future(model, column_name) %}
    {#
        Test that a timestamp column doesn't contain future dates.
    #}
    
    select
        {{ column_name }} as future_timestamp
    from {{ model }}
    where {{ column_name }} is not null
        and {{ column_name }} > current_timestamp()
    
{% endmacro %}


{% macro calculate_data_quality_score(
    has_primary_key,
    has_timestamps,
    has_required_fields,
    total_fields,
    null_fields
) %}
    {#
        Calculate a data quality score based on completeness and validity.
        
        Args:
            has_primary_key: Whether the primary key is present (0 or 1)
            has_timestamps: Whether required timestamps are present (0 or 1)
            has_required_fields: Whether all required fields are present (0 or 1)
            total_fields: Total number of fields
            null_fields: Number of null fields
            
        Returns:
            Float between 0 and 1
    #}
    
    (
        ({{ has_primary_key }} * 0.4) +
        ({{ has_timestamps }} * 0.2) +
        ({{ has_required_fields }} * 0.2) +
        (safe_divide({{ total_fields }} - {{ null_fields }}, {{ total_fields }}) * 0.2)
    )
    
{% endmacro %}

