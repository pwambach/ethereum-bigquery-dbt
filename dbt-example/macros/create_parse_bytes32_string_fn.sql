{% macro create_parse_bytes32_string_fn() %}
    CREATE OR REPLACE FUNCTION 
        {{ target.schema }}.parse_bytes32_string(bytes STRING) returns STRING
    LANGUAGE js 

    AS """
    return ethersStrings.parseBytes32String(bytes);
    """
    options (LIBRARY = "gs://YOUR_BUCKET/ethersStrings-v5.7.0.js");

{% endmacro %}

