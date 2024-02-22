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
        ) AS altas_atendimento_raps_antes_nao_apos_sim
	FROM internacoes internacao
	-- Apenas as internações que tiveram alta
	WHERE internacao.desfecho_motivo_id_sihsus IN (
        '11',  -- Alta curado
        '12',  -- Alta melhorado
        '14',  -- Alta a pedido
        '15',  -- Alta com previsão de retorno p/acomp do paciente
        '16',  -- Alta por evasão
        '18',  -- Alta por outros motivos
        '19',  -- Alta de paciente agudo em psiquiatria
        '29',  -- Transferência para internação domiciliar
        '32',  -- Transferência para internação domiciliar
        '51'   -- Encerramento administrativo
    )
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

{{  revelar_combinacoes_implicitas(
    relacao="subtotais_antes_apos",
    agrupar_por=[

    ],
    colunas_a_completar=[
        ["periodo_data_inicio"],
        ["unidade_geografica_id", "unidade_geografica_id_sus"]
    ],
    cte_resultado="contagem_antes_apos_com_combinacoes"
) }},

resumo_com_percentuais AS (
	SELECT		
		periodo_data_inicio,
		unidade_geografica_id,
		unidade_geografica_id_sus,
		coalesce(altas_atendimento_raps_antes_nao_apos_nao, 0) AS altas_atendimento_raps_antes_nao_apos_nao,
		coalesce(altas_atendimento_raps_antes_sim_apos_nao, 0) AS altas_atendimento_raps_antes_sim_apos_nao,
		coalesce(altas_atendimento_raps_antes_sim_apos_sim, 0) AS altas_atendimento_raps_antes_sim_apos_sim,
		coalesce(altas_atendimento_raps_antes_nao_apos_sim, 0) AS altas_atendimento_raps_antes_nao_apos_sim,
		now() AS atualizacao_data,
		coalesce(altas_atendimento_raps_6m_antes, 0) AS altas_atendimento_raps_6m_antes,
		coalesce(altas_atendimento_raps_1m_apos, 0) AS altas_atendimento_raps_1m_apos,
		coalesce(altas_total, 0) AS altas_total,
		round(
			100 * coalesce(altas_atendimento_raps_6m_antes, 0)::numeric
			/ nullif(altas_total, 0),
			1
		) AS perc_altas_atendimento_raps_6m_antes,
		round(
			100 * coalesce(altas_atendimento_raps_1m_apos, 0)::numeric
			/ nullif(altas_total, 0),
			1
		) AS perc_altas_atendimento_raps_1m_apos
	FROM contagem_antes_apos_com_combinacoes
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
