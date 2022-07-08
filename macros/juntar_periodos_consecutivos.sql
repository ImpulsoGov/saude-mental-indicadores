{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    juntar_periodos_consecutivos(
        relacao,
        agrupar_por,
        colunas_valores,
        periodo_tipo="Mensal",
        coluna_periodo="periodo_id",
        colunas_adicionais_periodo=["periodo_data_inicio"],
        cte_resultado="com_periodo_anterior"
    )
-%}
{{ relacao }}_competencia_referencia AS (
SELECT
{%- for coluna in agrupar_por %}
    dados_competencia_referencia.{{ coluna }} AS {{ coluna }},
{%- endfor %}
    {{ coluna_periodo }},
{%- for coluna in colunas_adicionais_periodo %}
    sucessao.{{ coluna }} AS {{ coluna }},
{%- endfor %}
{%- for coluna in colunas_valores %}
    dados_competencia_referencia.{{ coluna }} AS {{ coluna }}
    {{- "," if not loop.last }}
{%- endfor %}
FROM {{ relacao }} AS dados_competencia_referencia
LEFT JOIN {{ ref('periodos_sucessao') }} AS sucessao
USING ( {{ coluna_periodo }} )
WHERE sucessao.periodo_tipo = '{{ periodo_tipo }}'
),
{{ relacao }}_competencia_anterior AS (
SELECT
{%- for coluna in agrupar_por %}
    dados_competencia_anterior.{{ coluna }} AS {{ coluna }},
{%- endfor %}
    sucessao.{{ coluna_periodo }} AS {{ coluna_periodo }},
{%- for coluna in colunas_adicionais_periodo %}
    sucessao.{{ coluna }} AS {{ coluna }},
{%- endfor %}
{%- for coluna in colunas_valores %}
    dados_competencia_anterior.{{ coluna }} AS {{ coluna }}
    {{- "," if not loop.last }}
{%- endfor %}
FROM {{ relacao }} AS dados_competencia_anterior
LEFT JOIN {{ ref('periodos_sucessao') }} AS sucessao
ON
    dados_competencia_anterior.{{ coluna_periodo }}
    = sucessao.ultimo_{{ coluna_periodo }}
WHERE sucessao.periodo_tipo = '{{ periodo_tipo }}'
),
{{ cte_resultado }} AS (
SELECT
{%- for coluna in (
    agrupar_por + [coluna_periodo] + colunas_adicionais_periodo
) %}
    coalesce(
        competencia_referencia.{{ coluna }},
        competencia_anterior.{{ coluna }}
    ) AS {{ coluna }},
{%- endfor %}
{%- for coluna in colunas_valores %}
    coalesce(
        competencia_referencia.{{ coluna }},
        0
    ) AS {{ coluna }},
    coalesce(
        competencia_anterior.{{ coluna }},
        0
    ) AS {{ coluna }}_anterior{{ "," if not loop.last }}
{%- endfor %}
FROM {{relacao}}_competencia_referencia competencia_referencia
FULL JOIN {{relacao}}_competencia_anterior competencia_anterior
USING (
{%- for coluna in (
    agrupar_por + [coluna_periodo] + colunas_adicionais_periodo
) %}
    {{ coluna }}{{ "," if not loop.last }}
{%- endfor %}
)
)
{%- endmacro -%}
