{% macro clickhouse__load_csv_rows(model, agate_table) %}
  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set data_sql = adapter.get_csv_data(agate_table) %}

  {% set sql -%}
    insert into {{ this.render() }} ({{ cols_sql }})
    {{ adapter.get_model_settings(model) }}
    format CSV
    {{ data_sql }}
  {%- endset %}

  {% do adapter.add_query(sql, bindings=agate_table, abridge_sql_log=True) %}
{% endmacro %}

{% macro clickhouse__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {%- set is_cluster = adapter.is_clickhouse_cluster_mode() -%}

  {% set sql %}
    create table {{ this.render() }}{% if is_cluster -%}_local{%- endif %} {{ on_cluster_clause()}} (
      {%- for col_name in agate_table.column_names -%}
        {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
        {%- set type = column_override.get(col_name, inferred_type) -%}
        {%- set column_name = (col_name | string) -%}
          {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%}
    )
    {{ engine_clause() }}
    {{ order_cols(label='order by') }}
    {{ partition_cols(label='partition by') }}
  {% endset %}

  {{ log(sql) }}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {% if is_cluster -%}
    {%- set sharding_key = model['config'].get('sharding_key') -%}
    {% call statement('_') -%}
      create table if not exists {{ this.render() }} {{ on_cluster_clause()}} as {{ this.render() }}_local
        engine = Distributed({{ adapter.get_clickhouse_cluster_name() }}, {{ this.render().split(".")[0] }}, {{ this.render().split(".")[1] }}_local, sipHash64({{ sharding_key }}))
    {%- endcall %}
  {%- endif %}

  {{ return(sql) }}
{% endmacro %}
