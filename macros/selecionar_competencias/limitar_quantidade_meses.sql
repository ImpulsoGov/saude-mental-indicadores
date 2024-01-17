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

{% set necessario_tabela_periodos = coluna_data == "periodo_id" %}

{% if necessario_tabela_periodos %}
    ultimos_meses_municipio AS (
        SELECT 
            umm_sem_periodo_id.*,
            periodos.id as periodo_id
        FROM (
            SELECT
                {{ coluna_municipio_id }},
                MAX(periodos.data_inicio) as max_data
            FROM {{ relacao }} t
            LEFT JOIN {{ source("codigos", "periodos") }} periodos ON t.periodo_id = periodos.id
            GROUP BY {{ coluna_municipio_id }}
        ) umm_sem_periodo_id
        LEFT JOIN {{ source("codigos", "periodos") }} periodos ON umm_sem_periodo_id.max_data = periodos.data_inicio
        ),
{% else %}
    ultimos_meses_municipio AS (
        SELECT
            {{ coluna_municipio_id }},
            MAX({{ coluna_data }}) as max_data
        FROM {{ relacao }} t
        LEFT JOIN {{ source("codigos", "periodos") }} periodos ON t.periodo_id = periodos.id
        GROUP BY {{ coluna_municipio_id }}
    ),
{% endif %}

{{ cte_resultado }} AS (
    SELECT 
        t.*
    FROM {{ relacao }} t
    JOIN ultimos_meses_municipio ON t.{{ coluna_municipio_id }} = ultimos_meses_municipio.{{ coluna_municipio_id }}
    {% if necessario_tabela_periodos %}
        LEFT JOIN {{ source("codigos", "periodos") }} periodos ON t.periodo_id = periodos.id
        WHERE periodos.data_inicio >= (ultimos_meses_municipio.max_data - INTERVAL '{{ quantidade_meses_a_limitar }} months'::interval)     
    {% else %}
        WHERE t.{{ coluna_data }} >= (ultimos_meses_municipio.max_data - INTERVAL '{{ quantidade_meses_a_limitar }} months'::interval) 
    {% endif %}    
)

{% endmacro %}

