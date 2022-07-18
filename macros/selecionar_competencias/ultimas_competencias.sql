{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


{%- macro
    ultimas_competencias(
        relacao,
        fontes,
        meses_antes_ultima_competencia=(0, none),
        cte_resultado="ate_ultima_competencia"
    )
-%}
{{ cte_resultado }} AS (
SELECT
    dados.*
FROM {{ relacao }} AS dados
{%- for fonte in fontes %}
LEFT JOIN
    {{ ref(fonte + '_ultima_competencia') }} AS {{fonte}}_ultima_competencia
USING ( unidade_geografica_id )
{%- endfor %}
WHERE
{%- if meses_antes_ultima_competencia[0] is not none %}
    dados.periodo_data_inicio <= date_trunc(
        'month',
        LEAST(
        {%- for fonte in fontes %}
            {{fonte}}_ultima_competencia.periodo_data_inicio
            {{- "," if not loop.last}}
        {%- endfor %}
        )
        {%- if meses_antes_ultima_competencia[0] > 0 %}
        - "{{ meses_antes_ultima_competencia[0] }} months"::interval
        {%- endif %}
    )
{%- endif %}
{%- if (
        meses_antes_ultima_competencia[0] is not none
    and meses_antes_ultima_competencia[1] is not none
) %}
AND
{%- endif %}
{%- if meses_antes_ultima_competencia[1] is not none %}
    dados.periodo_data_inicio >= date_trunc(
        'month',
        LEAST(
        {%- for fonte in fontes %}
            {{fonte}}_ultima_competencia.periodo_data_inicio
            {{- "," if not loop.last}}
        {%- endfor %}
        )
        {%- if meses_antes_ultima_competencia[1] > 0 %}
        - "{{ meses_antes_ultima_competencia[0] }} months"::interval
        {%- endif %}
    )
{%- endif %}
)
{%- endmacro -%}
