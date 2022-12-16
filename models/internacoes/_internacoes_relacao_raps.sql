{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
internacoes_saude_mental AS (
    SELECT * FROM {{ ref("_internacoes_saude_mental") }}
),
raps_usuarios_atendidos AS (
    SELECT * FROM {{ ref("_raps_usuarios_atendidos") }}
),
internados_atendimentos_raps AS (
    SELECT
        internacao.*,
        atendimento_raps.usuario_id_cns_criptografado
            AS atendimento_usuario_id_cns_criptografado,
        atendimento_raps.periodo_data_inicio AS atendimento_periodo_data_inicio
    FROM internacoes_saude_mental internacao
    LEFT JOIN raps_usuarios_atendidos atendimento_raps
	ON
        internacao.usuario_sexo_id_sigtap
	    = atendimento_raps.usuario_sexo_id_sigtap
	AND internacao.usuario_nascimento_data
	    = atendimento_raps.usuario_nascimento_data
	AND internacao.usuario_residencia_municipio_id_sus
	    = atendimento_raps.usuario_residencia_municipio_id_sus
    -- Considera apenas atendimentos de rotina; atendimentos de urgência em 
    -- outros dispositivos; não são considerados como continuidade do cuidado
    WHERE quantidade_registrada_rotina > 0
),
internados_atendimentos_raps_antes AS (
    SELECT
        internacao.id,
        -- checa se houve pelo menos uma correspondência com atendimentos na
        -- RAPS usando os campos na clausula LEFT JOIN
        (
            CASE WHEN count(
                internacao.atendimento_usuario_id_cns_criptografado
            ) > 0 THEN true
            ELSE false
            END
         ) AS atendimento_raps_6m_antes
    FROM internados_atendimentos_raps internacao
	-- procedimento realizado na RAPS até 6 meses antes do início da AIH
	WHERE internacao.atendimento_periodo_data_inicio
	    >= (
	       date_trunc('month', internacao.aih_data_inicio) - '6 mon'::INTERVAL
	   )::date
	AND atendimento_periodo_data_inicio
	   < date_trunc('month', internacao.aih_data_inicio)
	GROUP BY internacao.id
),
internados_atendimentos_raps_apos AS (
    SELECT
        internacao.id,
        -- checa se houve pelo menos uma correspondência com atendimentos na
        -- RAPS usando as condições na clausula LEFT JOIN
        (
            CASE WHEN count(
                internacao.atendimento_usuario_id_cns_criptografado
            ) > 0 THEN true
            ELSE false
            END
         ) AS atendimento_raps_1m_apos
    FROM internados_atendimentos_raps internacao
    -- procedimento realizado a partir do mês de término da internação...
    WHERE atendimento_periodo_data_inicio >= date_trunc(
        'month',
        internacao.aih_data_fim
    )
    -- e no máximo até a competência seguinte à alta da AIH
    AND internacao.atendimento_periodo_data_inicio <= (
            date_trunc('month', internacao.aih_data_fim) + '1 mon'::interval
    )::date
    -- Só faz sentido avaliar a ida à RAPS após o desfecho da internação se o
    -- desfecho for alta
    AND internacao.desfecho_motivo_id_sihsus IN (
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
    GROUP BY internacao.id
),
relacao_raps AS (
    SELECT
        internacoes_saude_mental.*,
        coalesce(
            internados_atendimentos_raps_antes.atendimento_raps_6m_antes,
            false
        ) AS atendimento_raps_6m_antes,
        coalesce(
            internados_atendimentos_raps_apos.atendimento_raps_1m_apos,
            false
        ) AS atendimento_raps_1m_apos
    FROM internacoes_saude_mental
    LEFT JOIN internados_atendimentos_raps_antes
    USING ( id )
    LEFT JOIN internados_atendimentos_raps_apos
    USING ( id )
),
-- As seis primeiras competências disponíveis devem ser descartadas, já que
-- nelas o cálculo dos usuários que foram atendidos antes da internação é
-- baseado em dados incompletos
{{ primeiras_competencias(
    relacao="relacao_raps",
    fontes=[
        "aih_rd_disseminacao",
        "bpa_i_disseminacao",
        "raas_psicossocial_disseminacao"
    ],
    meses_apos_primeira_competencia=(7, none),
    cte_resultado="apos_primeiro_semestre"
) }},
-- A última competência disponível deve ser descartada, já que nela o cálculo
-- dos usuários que foram atendidos após a alta é baseado em dados incompletos
{{ ultimas_competencias(
    relacao="apos_primeiro_semestre",
    fontes=[
        "aih_rd_disseminacao",
        "bpa_i_disseminacao",
        "raas_psicossocial_disseminacao"
    ],
    meses_antes_ultima_competencia=(1, none),
    cte_resultado="final"
) }}
SELECT * FROM final
