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
internacoes_duracao AS (
    SELECT * FROM {{ ref("internacoes_duracoes") }}
),
{{ cte_resultado }} AS (
    SELECT 
        t.*,
        {% for coluna in colunas_internacao_duracao -%}
        internacao_duracao.{{- coluna }} AS internacao_duracao_{{- coluna -}}
        {{- "," if not loop.last }}
        {%- endfor %}
    FROM {{ relacao }} t
    LEFT JOIN internacoes_duracao internacao_duracao
    ON
        {{ duracao }} >= internacao_duracao.minimo
    AND {{ duracao }} < internacao_duracao.maximo
)
{%- endmacro -%}
