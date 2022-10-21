{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    classificar_faixa_etaria(
        relacao,
        coluna_nascimento_data,
        coluna_data_referencia,
        idade_tipo="Anos",
        faixa_etaria_agrupamento="10 em 10 anos",
        colunas_faixa_etaria=["id", "descricao", "ordem"],
        cte_resultado="com_faixa_etaria"
    )
-%}
{%- set idade -%}
AGE(t.{{ coluna_data_referencia }}, t.{{ coluna_nascimento_data }})
{%- endset -%}
{%- set
    partes_data = {
        "Minutos": "MINUTE",
        "Horas": "HOUR",
        "Dias": "DAY",
        "Meses": "MONTH",
        "Anos": "YEAR"
    }
-%}
faixas_etarias AS (
    SELECT * FROM {{ source("codigos", "faixas_etarias") }}
),
faixas_etarias_agrupamentos AS (
    SELECT * FROM {{ source("codigos", "faixas_etarias_agrupamentos") }}
),
{{ cte_resultado }} AS (
    SELECT 
        t.*,
        {% for coluna in colunas_faixa_etaria -%}
        faixa_etaria.{{- coluna }} AS faixa_etaria_{{- coluna -}}
        {{- "," if not loop.last }}
        {%- endfor %}
    FROM {{ relacao }} t
    LEFT JOIN faixas_etarias faixa_etaria
    ON 
        EXTRACT(
            '{{ partes_data[idade_tipo] }}' FROM {{ idade }}
        ) >= faixa_etaria.idade_minima
    AND EXTRACT(
            '{{ partes_data[idade_tipo] }}' FROM {{ idade }}
        ) < faixa_etaria.idade_maxima
    AND faixa_etaria.idade_tipo = '{{ idade_tipo }}'
    LEFT JOIN faixas_etarias_agrupamentos faixa_etaria_agrupamento
    ON faixa_etaria.agrupamento_id = faixa_etaria_agrupamento.id
    WHERE faixa_etaria_agrupamento.descricao = '{{ faixa_etaria_agrupamento }}'
)
{%- endmacro -%}
