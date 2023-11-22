{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro nomear_ocupacoes(
    relacao,
    coluna_ocupacao_nome="ocupacao_nome",
    coluna_ocupacao_id="ocupacao_id_cbo2002",
    todas_ocupacoes_id="000000",
    todas_ocupacoes_valor="Todas",
    cte_resultado="com_nomes_ocupacoes"
) %}
{%- set re = modules.re -%}
{%- set lista_de_codigos_colunas = adapter.get_columns_in_relation(
    source("codigos", "ocupacoes")
) -%}
{%- set lista_de_codigos_coluna_id = (
    "ocupacao" + re.match(".*ocupacao.*(_id.*)", coluna_ocupacao_id).groups(1)[0]
) -%}
{{ cte_resultado }} AS (
SELECT
    t.*,
    {% if todas_ocupacoes_id is not none -%}
    (
        CASE
            WHEN
            t.{{ coluna_ocupacao_id }} = '{{ todas_ocupacoes_id }}'
            THEN '{{ todas_ocupacoes_valor }}'
        ELSE
{%- endif %}    coalesce(ocupacao.ocupacao_descricao_cbo2002, ocupacao_sigtap.ocupacao_descricao_sigtap)
    {%- if todas_ocupacoes_id is not none %}
        END
    ){%- endif %} AS {{ coluna_ocupacao_nome }}
FROM {{ relacao }} t

LEFT JOIN (
    SELECT 
    {%- for coluna in lista_de_codigos_colunas %}
        {{coluna.quoted}} AS "ocupacao_{{coluna.name}}"
        {{- "," if not loop.last }}
    {%- endfor %}
    FROM {{ source("codigos", "ocupacoes") }}
) ocupacao
ON t.{{ coluna_ocupacao_id }} = ocupacao.{{ lista_de_codigos_coluna_id }}

LEFT JOIN (
    SELECT 
    {%- for coluna in lista_de_codigos_colunas %}
        {{coluna.quoted}} AS "ocupacao_{{coluna.name}}"
        {{- "," if not loop.last }}
    {%- endfor %}
    FROM {{ source("codigos", "ocupacoes") }}
) ocupacao_sigtap
ON t.{{ coluna_ocupacao_id }} = ocupacao_sigtap.ocupacao_id_sigtap

)
{%- endmacro -%}
