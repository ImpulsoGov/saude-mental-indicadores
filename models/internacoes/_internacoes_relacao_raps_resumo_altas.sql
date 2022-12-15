{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
internacoes AS (
	SELECT * FROM {{ ref("_internacoes_relacao_raps") }}
),
periodos AS (
	SELECT * FROM {{ source("codigos", "periodos") }}
),
contagem_antes_apos AS (
	SELECT
		date_trunc(
			'month',
			internacao.aih_data_fim
		)::date AS periodo_data_inicio,
		internacao.unidade_geografica_id,
		internacao.unidade_geografica_id_sus,
		count(DISTINCT internacao.id) FILTER (
		  WHERE 
		      NOT atendimento_raps_6m_antes 
		  AND NOT atendimento_raps_1m_apos
		) AS altas_atendimento_raps_antes_nao_apos_nao,
		count(DISTINCT internacao.id) FILTER (
            WHERE 
                    atendimento_raps_6m_antes
            AND NOT atendimento_raps_1m_apos
        ) AS altas_atendimento_raps_antes_sim_apos_nao,
		count(DISTINCT internacao.id) FILTER (
            WHERE 
                atendimento_raps_6m_antes
            AND atendimento_raps_1m_apos
        ) AS altas_atendimento_raps_antes_sim_apos_sim,
		count(DISTINCT internacao.id) FILTER (
            WHERE
            NOT atendimento_raps_6m_antes
            AND atendimento_raps_1m_apos
        ) AS altas_atendimento_raps_antes_nao_apos_sim,
		max(atualizacao_data) AS atualizacao_data
	FROM internacoes internacao
	-- Apenas as internações que tiveram alta (reconhecidas porque as 
	-- internações que não tiveram alta não tem valor definido para o campo
	-- calculado `atendimento_raps_1m_apos`)
	WHERE atendimento_raps_1m_apos IS NOT NULL
	GROUP BY
		internacao.unidade_geografica_id,
		internacao.unidade_geografica_id_sus,
		date_trunc('month', internacao.aih_data_fim)::date
	{% if is_incremental() %}
	HAVING date_trunc('month', internacao.aih_data_fim)::date > (
		SELECT max(periodo_data_inicio) FROM {{ this }}
	)
	{% endif %}
),
subtotais_antes_apos AS (
	SELECT
		*,
		(
            altas_atendimento_raps_antes_sim_apos_nao 
            + altas_atendimento_raps_antes_sim_apos_sim
        ) AS altas_atendimento_raps_6m_antes,
		(
            altas_atendimento_raps_antes_nao_apos_sim 
            + altas_atendimento_raps_antes_sim_apos_sim
        ) AS altas_atendimento_raps_1m_apos,
		(
			altas_atendimento_raps_antes_nao_apos_nao
			+ altas_atendimento_raps_antes_sim_apos_nao
			+ altas_atendimento_raps_antes_sim_apos_sim
			+ altas_atendimento_raps_antes_nao_apos_sim
		) AS altas_total
	FROM contagem_antes_apos
),
resumo_com_percentuais AS (
	SELECT
		*,
		round(
			100 * altas_atendimento_raps_6m_antes::numeric
			/ nullif(altas_total, 0),
			1
		) AS perc_altas_atendimento_raps_6m_antes,
		round(
			100 * altas_atendimento_raps_1m_apos::numeric
			/ nullif(altas_total, 0),
			1
		) AS perc_altas_atendimento_raps_1m_apos
	FROM subtotais_antes_apos
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
