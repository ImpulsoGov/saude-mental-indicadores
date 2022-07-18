{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    classificar_faixa_etaria(
        relacao,
        coluna_data_nascimento,
        coluna_data_referencia,
        idade_tipo="Anos",
        colunas_faixa_etaria=["id", "descricao", "ordem"],
        cte_resultado="com_faixa_etaria"
    )
-%}
{%- set idade -%}
AGE(t.{{ coluna_data_referencia }}, t.{{ coluna_data_nascimento }})
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
)
{%- endmacro -%}
