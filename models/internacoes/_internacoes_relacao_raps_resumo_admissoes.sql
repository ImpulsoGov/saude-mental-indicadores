{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
periodos AS (
	SELECT * FROM {{ source("codigos", "periodos") }}
),
internacoes AS (
	SELECT * FROM {{ ref("_internacoes_relacao_raps") }}
),
contagem_admissoes AS (
	SELECT
		date_trunc(
			'month',
			internacao.aih_data_inicio
		)::date AS periodo_data_inicio,
		internacao.unidade_geografica_id,
		internacao.unidade_geografica_id_sus,
    	count(DISTINCT internacao.aih_id_sihsus) FILTER (
    	   WHERE atendimento_raps_6m_antes
    	) AS internacoes_atendimento_raps_antes,
    	count(DISTINCT internacao.aih_id_sihsus) FILTER (
    	   WHERE 
    	       internacao.condicao_saude_mental_classificao 
    	       = 'Álcool e outras drogas'
    	) AS internacoes_alcool_drogas,
    	count(DISTINCT internacao.aih_id_sihsus) FILTER (
    	   WHERE
    	       internacao.condicao_saude_mental_classificao 
    	       != 'Álcool e outras drogas'
    	   ) AS internacoes_transtornos,
    	count(DISTINCT internacao.id) AS internacoes_total,
		max(atualizacao_data) AS atualizacao_data
	FROM internacoes internacao
	GROUP BY
		internacao.unidade_geografica_id,
		internacao.unidade_geografica_id_sus,
		date_trunc('month', internacao.aih_data_inicio)::date
	{% if is_incremental() %}
	HAVING date_trunc('month', internacao.aih_data_inicio)::date > (
		SELECT max(periodo_data_inicio) FROM {{ this }}
	)
	{% endif %}
),
resumo_com_percentuais AS (
	SELECT
		*,
		round(
			100 * internacoes_atendimento_raps_antes::numeric
			/ nullif(internacoes_total, 0),
			1
		) AS perc_internacoes_atendimento_raps_antes
	FROM contagem_admissoes
),
final AS (
	SELECT
		{{ dbt_utils.surrogate_key([
			"resumo_com_percentuais.unidade_geografica_id",
			"periodo.id"
		]) }} AS id,
		periodo.id AS periodo_id,
		resumo_com_percentuais.*
	FROM resumo_com_percentuais
	LEFT JOIN periodos periodo
	ON resumo_com_percentuais.periodo_data_inicio = periodo.data_inicio
	AND periodo.tipo = 'Mensal'
)
SELECT * FROM final
