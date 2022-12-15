{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


{%- macro
    primeiras_competencias(
        relacao,
        fontes,
        meses_apos_primeira_competencia=(0, none),
        cte_resultado="a_partir_primeira_competencia"
    )
-%}
{{ cte_resultado }} AS (
SELECT
    dados.*
FROM {{ relacao }} AS dados
{%- for fonte in fontes %}
LEFT JOIN
    {{ ref(fonte + '_primeira_competencia') }}
        AS {{fonte}}_primeira_competencia
USING ( unidade_geografica_id )
{%- endfor %}
WHERE
{%- if meses_apos_primeira_competencia[0] is not none %}
    dados.periodo_data_inicio >= date_trunc(
        'month',
        GREATEST(
        {%- for fonte in fontes %}
            {{fonte}}_primeira_competencia.periodo_data_inicio
            {{- "," if not loop.last}}
        {%- endfor %}
        )
        {%- if meses_apos_primeira_competencia[0] > 0 %}
        + '{{ meses_apos_primeira_competencia[0] }} months'::interval
        {%- endif %}
    )
{%- endif %}
{%- if (
        meses_apos_primeira_competencia[0] is not none
    and meses_apos_primeira_competencia[1] is not none
) %}
AND
{%- endif %}
{%- if meses_apos_primeira_competencia[1] is not none %}
    dados.periodo_data_inicio <= date_trunc(
        'month',
        GREATEST(
        {%- for fonte in fontes %}
            {{fonte}}_apos_primeira_competencia.periodo_data_inicio
            {{- "," if not loop.last}}
        {%- endfor %}
        )
        {%- if meses_apos_primeira_competencia[1] > 0 %}
        + '{{ meses_apos_primeira_competencia[1] }} months'::interval
        {%- endif %}
    )
{%- endif %}
)
{%- endmacro -%}
