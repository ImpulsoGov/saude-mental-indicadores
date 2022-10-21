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
{%- set colunas_todas = (
    agrupar_por + colunas_a_totalizar + agregacoes_valores.keys()|list
) -%}
{{ cte_resultado }} AS (
    SELECT       
{%- for coluna in agrupar_por %}
    {{ coluna }},
{%- endfor %}
{%- for coluna_a_totalizar in colunas_a_totalizar %}
{%- set nome_categoria_com_totais = (
    
) %}
    coalesce(
        {{ coluna_a_totalizar }},
        '{{ nomes_categorias_com_totais[loop.index-1] }}'
    ) AS {{ coluna_a_totalizar }},
{%- endfor %}
{%- for coluna, agregacao in agregacoes_valores.items() %}
    {%- set agregacao_ = agregacao|trim|lower %}
    {{ agregacao_ }}{{- " " if agregacao_.endswith("order by") else "(" -}} 
    {{- coluna }}) AS {{ coluna }}{{ "," if not loop.last }}
{%- endfor %}
FROM {{ relacao }}
GROUP BY 
{%- for coluna in agrupar_por %}
    {{ coluna }},
{%- endfor %}
    CUBE(
    {%- for coluna in colunas_a_totalizar %}
        {{ coluna }}{{ "," if not loop.last }}
    {%- endfor %}
    )
)
{%- endmacro -%}
