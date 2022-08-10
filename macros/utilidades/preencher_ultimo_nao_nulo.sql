{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro preencher_ultimo_nao_nulo(
    relacao,
    agrupar_por,
    ordenar_por,
    colunas_a_preencher,
    colunas_manter=[],
    cte_resultado="com_ultimo_nao_nulo"
) %}
{# 
Macro inspirada nesta resposta do DBA StackExchange:
https://dba.stackexchange.com/a/186244
#}
{{ relacao }}_preencher_ultimo_nao_nulo_particoes AS (
SELECT
    *,
    {%- for coluna_a_preencher in colunas_a_preencher %}
    count({{ coluna_a_preencher }}) OVER (
        PARTITION BY 
        {%- for coluna in agrupar_por %}
            {{ coluna }}{{ "," if not loop.last }}
        {%- endfor %} 
        ORDER BY
        {%- for coluna in ordenar_por %}
            {{ coluna }}{{ "," if not loop.last }}
        {%- endfor %}
    ) AS {{coluna_a_preencher }}_particao{{ "," if not loop.last }}
    {%- endfor %}
FROM {{ relacao }}
),
{{ cte_resultado }} AS (
SELECT
{%- for coluna in agrupar_por %}
    {{ coluna }},
{%- endfor %}
{%- for coluna in ordenar_por %}
    {{ coluna }},
{%- endfor %}
{%- for coluna in colunas_manter %}
    {{ coluna }},
{%- endfor %}
    {%- for coluna_a_preencher in colunas_a_preencher %}
    first_value({{ coluna_a_preencher }}) OVER (
        PARTITION BY
        {%- for coluna in agrupar_por %}
            {{ coluna }},
        {%- endfor %}
            {{ coluna_a_preencher }}_particao
        ORDER BY
        {%- for coluna in ordenar_por %}
            {{ coluna }}{{ "," if not loop.last }}
        {%- endfor %}
    ) AS {{ coluna_a_preencher }}{{ "," if not loop.last }}
    {%- endfor %}
FROM {{ relacao }}_preencher_ultimo_nao_nulo_particoes
)
{%- endmacro -%}