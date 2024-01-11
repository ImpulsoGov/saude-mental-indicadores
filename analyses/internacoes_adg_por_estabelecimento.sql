WITH
internacoes_saude_mental AS (
    SELECT * FROM "principal"."_saude_mental_producao"."_internacoes_saude_mental"
),
raps_usuarios_atendidos AS (
    SELECT * FROM "principal"."_saude_mental_producao"."_raps_usuarios_atendidos"
),
periodos AS (
	SELECT * FROM principal.listas_de_codigos.periodos
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
apos_primeiro_semestre AS (
SELECT
    dados.*
FROM relacao_raps AS dados
LEFT JOIN
    "principal"."_saude_mental_producao"."_aih_rd_disseminacao_primeira_competencia"
        AS aih_rd_disseminacao_primeira_competencia
USING ( unidade_geografica_id )
LEFT JOIN
    "principal"."_saude_mental_producao"."_bpa_i_disseminacao_primeira_competencia"
        AS bpa_i_disseminacao_primeira_competencia
USING ( unidade_geografica_id )
LEFT JOIN
    "principal"."_saude_mental_producao"."_raas_psicossocial_disseminacao_primeira_competencia"
        AS raas_psicossocial_disseminacao_primeira_competencia
USING ( unidade_geografica_id )
WHERE
    dados.periodo_data_inicio >= date_trunc(
        'month',
        GREATEST(
            aih_rd_disseminacao_primeira_competencia.periodo_data_inicio,
            bpa_i_disseminacao_primeira_competencia.periodo_data_inicio,
            raas_psicossocial_disseminacao_primeira_competencia.periodo_data_inicio
        )
        + '7 months'::interval
    )
),
-- A última competência disponível deve ser descartada, já que nela o cálculo
-- dos usuários que foram atendidos após a alta é baseado em dados incompletos
internacoes AS (
SELECT
    dados.*
FROM apos_primeiro_semestre AS dados
LEFT JOIN
    "principal"."_saude_mental_producao"."_aih_rd_disseminacao_ultima_competencia" AS aih_rd_disseminacao_ultima_competencia
USING ( unidade_geografica_id )
LEFT JOIN
    "principal"."_saude_mental_producao"."_bpa_i_disseminacao_ultima_competencia" AS bpa_i_disseminacao_ultima_competencia
USING ( unidade_geografica_id )
LEFT JOIN
    "principal"."_saude_mental_producao"."_raas_psicossocial_disseminacao_ultima_competencia" AS raas_psicossocial_disseminacao_ultima_competencia
USING ( unidade_geografica_id )
WHERE
    dados.periodo_data_inicio <= date_trunc(
        'month',
        LEAST(
            aih_rd_disseminacao_ultima_competencia.periodo_data_inicio,
            bpa_i_disseminacao_ultima_competencia.periodo_data_inicio,
            raas_psicossocial_disseminacao_ultima_competencia.periodo_data_inicio
        )
        - '1 months'::interval
    )
),


------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

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
internacoes_relacao_raps_resumo_altas AS (
	SELECT
		md5(cast(coalesce(cast(resumo_com_percentuais.unidade_geografica_id as TEXT), '') || '-' || coalesce(cast(periodo.id as TEXT), '') as TEXT)) AS id,
		periodo.id AS periodo_id,
		resumo_com_percentuais.*
	FROM resumo_com_percentuais
	LEFT JOIN periodos periodo
	ON resumo_com_percentuais.periodo_data_inicio = periodo.data_inicio
	AND periodo.tipo = 'Mensal'
),


-------------------------------------------------------------------------------
-------------------------------------------------------------------------------



periodo_data_inicio_ultimas_competencias AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        periodo_data_inicio AS ultima_competencia
    FROM internacoes_relacao_raps_resumo_altas
    ORDER BY
        unidade_geografica_id,
        periodo_data_inicio DESC
),
com_datas_legiveis AS (
    SELECT
        t.*,
        (
            CASE
                WHEN periodo_data_inicio = ultima_competencia
                THEN 'Último período'
            ELSE (
                CASE
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 1 THEN 'Jan/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 2 THEN 'Fev/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 3 THEN 'Mar/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 4 THEN 'Abr/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 5 THEN 'Mai/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 6 THEN 'Jun/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 7 THEN 'Jul/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 8 THEN 'Ago/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 9 THEN 'Set/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 10 THEN 'Out/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 11 THEN 'Nov/'
                    WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 12 THEN 'Dez/'
                END || to_char(periodo_data_inicio, 'YY')
            ) END
        )  AS periodo,
        (
            CASE
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 1 THEN 'Janeiro'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 2 THEN 'Fevereiro'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 3 THEN 'Março'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 4 THEN 'Abril'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 5 THEN 'Maio'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 6 THEN 'Junho'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 7 THEN 'Julho'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 8 THEN 'Agosto'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 9 THEN 'Setembro'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 10 THEN 'Outubro'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 11 THEN 'Novembro'
                WHEN EXTRACT ( MONTH FROM periodo_data_inicio ) = 12 THEN 'Dezembro'
            END
        ) AS nome_mes,
        (
            to_char(periodo_data_inicio, 'YY')::numeric + EXTRACT ( MONTH FROM periodo_data_inicio )/100
        ) AS periodo_ordem
    FROM internacoes_relacao_raps_resumo_altas t
    LEFT JOIN periodo_data_inicio_ultimas_competencias
    USING (unidade_geografica_id)
),
final AS (
    SELECT
        id,
        periodo_id,
        periodo_data_inicio AS competencia,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        altas_atendimento_raps_antes_nao_apos_nao,
        altas_atendimento_raps_antes_sim_apos_nao,
        altas_atendimento_raps_antes_sim_apos_sim,
        altas_atendimento_raps_antes_nao_apos_sim,
        atualizacao_data,
        altas_atendimento_raps_6m_antes,
        altas_atendimento_raps_1m_apos,
        altas_total,
        perc_altas_atendimento_raps_6m_antes,
        perc_altas_atendimento_raps_1m_apos,
        periodo,
        nome_mes,
        periodo_ordem
    FROM com_datas_legiveis
)
SELECT * FROM final;