{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
internacoes_relacao_raps_resumo_admissoes_12m AS (
	SELECT * FROM {{ ref("internacoes_relacao_raps_resumo_admissoes_12m") }}
),
final AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		a_partir_de_ano,
		a_partir_de_mes,
		ate_ano,
		ate_mes,
		'Sim' AS atendimento_raps_6m_antes,
		(perc_internacoes_atendimento_raps_antes) / 100 AS prop_internacoes
	FROM internacoes_relacao_raps_resumo_admissoes_12m
	UNION
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		a_partir_de_ano,
		a_partir_de_mes,
		ate_ano,
		ate_mes,
		'Não' AS atendimento_raps_6m_antes,
		(
			100 - perc_internacoes_atendimento_raps_antes
		) / 100 AS prop_internacoes
	FROM internacoes_relacao_raps_resumo_admissoes_12m
)
SELECT * FROM final
