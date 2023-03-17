{% macro create_decode_log_fn() %}
    CREATE OR REPLACE FUNCTION 
        {{ target.schema }}.decode_log(abi STRING, data STRING, topics ARRAY<STRING>) returns JSON
    LANGUAGE js 

    AS """
    const parsedAbi = JSON.parse(abi);
    const interface = new ethersAbi.Interface([parsedAbi]);
    const log = interface.parseLog({topics, data});

    return parsedAbi.inputs.reduce((result, input) => {
      const key = input.name;
      let arg = log.args[key];

      if (typeof arg === 'object' && arg._hex) {
        arg = arg.toString();
      }
  
      result[key] = arg;

      return result;
    }, {});
    """
    options (LIBRARY = "gs://YOUR_BUCKET/ethersAbi-v5.7.0.js");

{% endmacro %}

