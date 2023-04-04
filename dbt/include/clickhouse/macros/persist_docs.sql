{% macro one_alter_relation(relation, alter_comments) %}
  alter table {{ relation }} {{ on_cluster_clause() }} {{ alter_comments }}
{% endmacro %}

{% macro one_alter_column_comment(relation, column_name, comment) %}
  alter table {{ relation }} {{ on_cluster_clause() }} comment column {{ column_name }} '{{ comment }}'
{% endmacro %}

{% macro clickhouse__alter_relation_comment(relation, comment) %}
  alter table {{ relation }} {{ on_cluster_clause() }} modify comment '{{ comment }}'
{% endmacro %}

{% macro clickhouse__persist_docs(relation, model, for_relation, for_columns) %}
  {%- set alter_comments = [] %}

  {%- if for_relation and config.persist_relation_docs() and model.description -%}
    {% do alter_comments.append("modify comment '{comment}'".format(comment=model.description)) %}
  {%- endif -%}

  {%- if for_columns and config.persist_column_docs() and model.columns -%}
    {%- for column_name in model.columns -%}
      {%- set comment = model.columns[column_name]['description'] -%}
      {%- if comment %}
        {% do alter_comments.append("comment column {column_name} '{comment}'".format(column_name=column_name, comment=comment)) %}
      {%- endif %}
    {%- endfor -%}
  {%- endif -%}

  {%- if alter_comments | length > 0 -%}
    {% do run_query(one_alter_relation(relation, alter_comments|join(', '))) %}
  {%- endif -%}
{% endmacro %}
