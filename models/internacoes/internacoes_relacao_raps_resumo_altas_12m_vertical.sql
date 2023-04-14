{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
internacoes_relacao_raps_resumo_altas_12m AS (
	SELECT * FROM {{ ref("internacoes_relacao_raps_resumo_altas_12m") }}
),
intermediario AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		a_partir_de_ano,
		a_partir_de_mes,
		ate_ano,
		ate_mes,
		'Sim' AS atendimento_raps_1m_apos,
		(perc_altas_atendimento_raps_1m_apos) / 100 AS prop_altas
	FROM internacoes_relacao_raps_resumo_altas_12m
	UNION
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		a_partir_de_ano,
		a_partir_de_mes,
		ate_ano,
		ate_mes,
		'Não' AS atendimento_raps_1m_apos,
		(100 - perc_altas_atendimento_raps_1m_apos) / 100 AS prop_altas
	FROM internacoes_relacao_raps_resumo_altas_12m
),
final AS (
	SELECT
		{{ dbt_utils.surrogate_key([
				"unidade_geografica_id",
				"a_partir_de_ano",
				"a_partir_de_mes",
				"ate_ano",
				"ate_mes",
				"atendimento_raps_1m_apos"
		]) }} AS id,
		*
	FROM intermediario
)
SELECT * FROM final
