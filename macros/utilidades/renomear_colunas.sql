{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro renomear_colunas(
    relacao,
    colunas_a_renomear,
    prefixo_colunas=none
) -%}
{%- set colunas_originais = adapter.get_columns_in_relation(relacao) -%}
{%- for coluna in colunas_originais %}
{%- if coluna.name in colunas_a_renomear %}
{{- (prefixo_colunas + ".") if prefixo_colunas is not none -}}
{{- coluna.name }} AS {{ colunas_a_renomear[coluna.name] -}}
{%- else %}
{{- coluna.name }}
{%- endif -%}
{{- "," if not loop.last }}
{% endfor -%}
{%- endmacro -%}
