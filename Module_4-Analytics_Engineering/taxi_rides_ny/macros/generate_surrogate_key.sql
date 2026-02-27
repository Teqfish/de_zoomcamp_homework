{% macro generate_surrogate_key(columns) -%}
  {#-
    columns: a list of column expressions, e.g. ['vendor_id', 'pickup_datetime', 'dropoff_datetime']
    Returns: an md5 hash surrogate key as a string
  -#}

  {% if columns is string %}
    {% set columns = [columns] %}
  {% endif %}

  md5(
    {%- for col in columns -%}
      coalesce(cast({{ col }} as varchar), '')
      {%- if not loop.last -%} || '|' || {%- endif -%}
    {%- endfor -%}
  )
{%- endmacro %}
