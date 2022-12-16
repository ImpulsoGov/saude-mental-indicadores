{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_usuarios_ativos_perfil') }}

WITH
acolhimentos_noturnos_por_municipio_por_mes AS (
	SELECT * FROM {{ ref("_caps_acolhimentos_noturnos") }}
),
{{ ultimas_competencias(
    relacao="acolhimentos_noturnos_por_municipio_por_mes",
    fontes=[
        "aih_rd_disseminacao",
        "bpa_i_disseminacao",
        "raas_psicossocial_disseminacao"
    ],
    meses_antes_ultima_competencia=(1, 12),
    cte_resultado="ultimos_12m"
) }},
final AS (
	{%- set mes_min %}EXTRACT(MONTH FROM min(periodo_data_inicio)){%- endset %}
	{%- set mes_max %}EXTRACT(MONTH FROM max(periodo_data_inicio)){%- endset %}
	SELECT
		{{ dbt_utils.surrogate_key(["unidade_geografica_id"]) }} AS id,
		unidade_geografica_id,
		unidade_geografica_id_sus,
		EXTRACT(YEAR FROM min(periodo_data_inicio))::text AS a_partir_de_ano,
        (
            CASE
                WHEN {{ mes_min }} = 1 THEN 'Janeiro'
                WHEN {{ mes_min }} = 2 THEN 'Fevereiro'
                WHEN {{ mes_min }} = 3 THEN 'Março'
                WHEN {{ mes_min }} = 4 THEN 'Abril'
                WHEN {{ mes_min }} = 5 THEN 'Maio'
                WHEN {{ mes_min }} = 6 THEN 'Junho'
                WHEN {{ mes_min }} = 7 THEN 'Julho'
                WHEN {{ mes_min }} = 8 THEN 'Agosto'
                WHEN {{ mes_min }} = 9 THEN 'Setembro'
                WHEN {{ mes_min }} = 10 THEN 'Outubro'
                WHEN {{ mes_min }} = 11 THEN 'Novembro'
                WHEN {{ mes_min }} = 12 THEN 'Dezembro'
            END
		) AS a_partir_de_mes,
		EXTRACT(YEAR FROM max(periodo_data_inicio))::text AS ate_ano,
		(
            CASE
                WHEN {{ mes_max }} = 1 THEN 'Janeiro'
                WHEN {{ mes_max }} = 2 THEN 'Fevereiro'
                WHEN {{ mes_max }} = 3 THEN 'Março'
                WHEN {{ mes_max }} = 4 THEN 'Abril'
                WHEN {{ mes_max }} = 5 THEN 'Maio'
                WHEN {{ mes_max }} = 6 THEN 'Junho'
                WHEN {{ mes_max }} = 7 THEN 'Julho'
                WHEN {{ mes_max }} = 8 THEN 'Agosto'
                WHEN {{ mes_max }} = 9 THEN 'Setembro'
                WHEN {{ mes_max }} = 10 THEN 'Outubro'
                WHEN {{ mes_max }} = 11 THEN 'Novembro'
                WHEN {{ mes_max }} = 12 THEN 'Dezembro'
            END
		) AS ate_mes,
        sum(acolhimentos_noturnos) AS acolhimentos_noturnos,
        sum(acolhimentos_noturnos_diarias) AS acolhimentos_noturnos_diarias,
        max(atualizacao_data) AS atualizacao_data
	FROM ultimos_12m
	GROUP BY
		unidade_geografica_id,
		unidade_geografica_id_sus 
)
SELECT * FROM final
