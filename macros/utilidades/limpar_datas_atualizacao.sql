{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro limpar_datas_atualizacao(relacao, prefixo_colunas=none) -%}
{%- set colunas_todas = adapter.get_columns_in_relation(relacao) -%}
{%- set colunas_sem_datas_atualizacao = colunas_todas | rejectattr(
    "name",
    "in",
    ["criacao_data", "atualizacao_data"]
) -%}
{%- for coluna in colunas_sem_datas_atualizacao %}
{{ (prefixo_colunas + ".") if prefixo_colunas is not none -}}
{{- coluna.name }}{{ "," if not loop.last }}
{% endfor -%}
{%- endmacro -%}
