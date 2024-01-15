{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


{%- macro 
    limitar_quantidade_meses(
        relacao, 
        coluna_data="realizacao_periodo_data_inicio", 
        coluna_municipio_id="unidade_geografica_id_sus", 
        quantidade_meses_a_limitar=72,
        cte_resultado="com_meses_limitados"
    )
-%}

ultimos_meses_municipio AS (
    SELECT
        {{ coluna_municipio_id }},
        MAX({{ coluna_data }}) as max_data
    FROM {{ relacao }}
    GROUP BY {{ coluna_municipio_id }}
),
{{ cte_resultado }} AS (
    SELECT 
        t.*
    FROM {{ relacao }} t
    JOIN ultimos_meses_municipio ON t.{{ coluna_municipio_id }} = ultimos_meses_municipio.{{ coluna_municipio_id }}
    WHERE t.{{ coluna_data }} >= (ultimos_meses_municipio.max_data - INTERVAL '{{ quantidade_meses_a_limitar }} months'::interval) 
)

{% endmacro %}
