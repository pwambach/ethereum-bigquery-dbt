{% macro create_decode_call_fn() %}
    CREATE OR REPLACE FUNCTION 
        {{ target.schema }}.decode_call(abi STRING, data STRING) returns JSON
    LANGUAGE js 

    AS """
    const parsedAbi = JSON.parse(abi);
    const interface = new ethersAbi.Interface([parsedAbi]);
    const call = interface.parseTransaction({data, value: 0});

    return parsedAbi.inputs.reduce((result, input) => {
      const key = input.name;
      let arg = call.args[key];

      if (typeof arg === 'object' && arg._hex) {
        arg = arg.toString();
      }
  
      result[key] = arg;

      return result;
    }, {});
    """
    options (LIBRARY = "gs://YOUR_BUCKET/ethersAbi-v5.7.0.js");

{% endmacro %}

