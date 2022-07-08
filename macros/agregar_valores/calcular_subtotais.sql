{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    calcular_subtotais(
        relacao,
        agrupar_por,
        colunas_a_totalizar,
        nomes_categorias_com_totais,
        agregacoes_valores,
        manter_original=true,
        cte_resultado="subtotais"
    )
-%}
{%- set ctes_subtotais = [relacao] if manter_original else []  -%}
{%- set colunas_todas = (
    agrupar_por + colunas_a_totalizar + agregacoes_valores.keys()|list
) -%}
{%- for coluna_a_totalizar in colunas_a_totalizar -%}
{%- set
    nome_categoria_com_totais = nomes_categorias_com_totais[loop.index-1]
-%}
{%- set
    agrupar_fora_totalizada = 
        (agrupar_por+colunas_a_totalizar)
        | reject("equalto", coluna_a_totalizar)
        | list
-%}
{%- set subtotal_parcial = modules.re.sub(
    "\W", "_", (relacao + "_" + coluna_a_totalizar + "_subtotal")|lower|trim
) %}
{%- set _ = ctes_subtotais.append(subtotal_parcial) -%}
{{ totalizar_coluna(
        relacao=relacao,
        agrupar_por=agrupar_fora_totalizada,
        coluna_a_totalizar=coluna_a_totalizar,
        nome_categoria_com_totais=nome_categoria_com_totais,
        agregacoes_valores=agregacoes_valores,
        cte_resultado=subtotal_parcial
    )
}},
{% endfor -%}
{%- set subtotal_geral -%}{{relacao}}_subtotal_geral{%- endset -%}
{{subtotal_geral}} AS (
SELECT
{%- for coluna in agrupar_por %}
    {{ coluna }},
{%- endfor %}
{%- for coluna_a_totalizar in colunas_a_totalizar %}
{%- set nome_categoria_com_totais = (
    nomes_categorias_com_totais[loop.index-1]
) %}
    '{{ nome_categoria_com_totais }}' AS {{ coluna_a_totalizar }},
{%- endfor %}
{%- for coluna, agregacao in agregacoes_valores.items() %}
    {{ agregacao }}({{ coluna }}) AS {{ coluna }}{{ "," if not loop.last }}
{%- endfor %}
FROM {{ relacao }}
GROUP BY
{%- for coluna in agrupar_por %}
    {{ coluna }}{{ "," if not loop.last }}
{%- endfor %}
),
{% set _ = ctes_subtotais.append(subtotal_geral) -%}
{{ cte_resultado }} AS (
{%- for cte in ctes_subtotais %}
SELECT
{%- for coluna in colunas_todas %}
    {{ coluna }}{{ ","  if not loop.last }}
{%- endfor %}
FROM {{ cte }}
{{ "UNION" if not loop.last -}}
{%- endfor -%}
)
{%- endmacro -%}
