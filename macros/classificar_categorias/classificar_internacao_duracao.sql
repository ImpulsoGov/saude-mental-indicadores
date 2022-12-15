{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    classificar_internacao_duracao(
        relacao,
        coluna_entrada_data="aih_data_inicio",
        coluna_desfecho_data="aih_data_fim",
        colunas_internacao_duracao=["id", "descricao", "ordem"],
        cte_resultado="com_duracao_internacao"
    )
-%}
{%- set duracao -%}
AGE(t.{{ coluna_desfecho_data }}, t.{{ coluna_entrada_data }})
{%- endset -%}
duracoes_internacoes AS (
    SELECT * FROM {{ ref("internacoes_duracoes") }}
),
{{ cte_resultado }} AS (
    SELECT 
        t.*,
        {% for coluna in colunas_duracao_internacao -%}
        duracao_internacao.{{- coluna }} AS duracao_internacao_{{- coluna -}}
        {{- "," if not loop.last }}
        {%- endfor %}
    FROM {{ relacao }} t
    LEFT JOIN duracoes_internacoes duracao_internacao
    ON
        {{ duracao }} >= duracao_internacao.minimo
    AND {{ duracao }} < duracao_internacao.maximo
)
{%- endmacro -%}
