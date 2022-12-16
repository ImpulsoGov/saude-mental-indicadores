{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
internacoes_relacao_raps_resumo_admissoes AS (
	SELECT * FROM {{ ref("_internacoes_relacao_raps_resumo_admissoes") }}
),
{{ ultimas_competencias(
    relacao="internacoes_relacao_raps_resumo_admissoes",
    fontes=[
        "aih_rd_disseminacao",
        "bpa_i_disseminacao",
        "raas_psicossocial_disseminacao"
    ],
    meses_antes_ultima_competencia=(1, 12),
    cte_resultado="ultimos_12m"
) }},
soma_ultimos_12m AS (
	{%- set mes_min %}EXTRACT(MONTH FROM min(periodo_data_inicio)){%- endset %}
	{%- set mes_max %}EXTRACT(MONTH FROM max(periodo_data_inicio)){%- endset %}
	SELECT
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
		sum(
			internacoes_atendimento_raps_antes
		) AS internacoes_atendimento_raps_antes,
    	sum(internacoes_alcool_drogas) AS internacoes_alcool_drogas,
    	sum(internacoes_transtornos) AS internacoes_transtornos,
    	sum(internacoes_total) AS internacoes_total,
		max(atualizacao_data) AS atualizacao_data
	FROM ultimos_12m
	GROUP BY
		unidade_geografica_id,
		unidade_geografica_id_sus
),
final AS (
	SELECT
		{{ dbt_utils.surrogate_key(["unidade_geografica_id"]) }} AS id,
		*,
		round(
	  		100 * internacoes_atendimento_raps_antes::numeric / nullif(
				internacoes_total,
				0
			),
	   		1
		) AS perc_internacoes_atendimento_raps_antes
	FROM soma_ultimos_12m
)
SELECT * FROM final
