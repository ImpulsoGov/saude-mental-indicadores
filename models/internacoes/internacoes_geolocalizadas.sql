{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

WITH
ceps AS (
	SELECT * FROM {{ source("codigos", "ceps") }}
),
ceps_por_bairro AS (
    SELECT * FROM {{ ref("_ceps_por_bairro") }}
),
desfechos_grupos AS (
    SELECT * FROM {{ ref("internacoes_desfechos_grupos") }}
),
internacoes AS (
    SELECT * FROM {{ ref("_internacoes_relacao_raps") }}
    {%- if is_incremental() %}
    WHERE atualizacao_data > (SELECT max(atualizacao_data) FROM {{ this }})
    {% endif %}
),
internacoes_colunas_selecionadas AS (
	SELECT
        internacao.id,
		internacao.unidade_geografica_id,
		internacao.unidade_geografica_id_sus,
		internacao.aih_data_inicio,
		internacao.aih_data_fim,
		internacao.usuario_nascimento_data,
		internacao.usuario_sexo_id_sigtap,
		internacao.usuario_residencia_cep,
		desfecho_grupo.desfecho_grupo_descricao,
		internacao.atendimento_raps_6m_antes,
		internacao.atendimento_raps_1m_apos,
		internacao.condicao_saude_mental_classificao,
        internacao.atualizacao_data
	FROM internacoes internacao
	LEFT JOIN desfechos_grupos desfecho_grupo
    ON internacao.desfecho_motivo_id_sihsus = desfecho_grupo.desfecho_id_sihsus
),
{{ classificar_internacao_duracao(
    relacao="internacoes_colunas_selecionadas",
    coluna_entrada_data="aih_data_inicio",
    coluna_desfecho_data="aih_data_fim",
    colunas_internacao_duracao=["id", "descricao", "ordem"],
    cte_resultado="com_duracao_internacao"
) }},
internacoes_provavel_mesmo_usuario AS (
	SELECT 
		internacao_posterior.*,
		coalesce(
			age(
				internacao_posterior.aih_data_inicio,
				internacao_anterior.aih_data_fim
			) < '6 mon'::interval,
			FALSE
		) AS menos_6m_ultima_internacao
	FROM com_duracao_internacao internacao_anterior
	RIGHT JOIN com_duracao_internacao internacao_posterior
	USING (
		usuario_nascimento_data,
		usuario_sexo_id_sigtap,
        usuario_residencia_cep
    )
	WHERE
        internacao_anterior.aih_data_fim < internacao_posterior.aih_data_inicio
),
internacoes_com_cep AS (
    SELECT
        internacao.id,
    	ceps_por_municipio.unidade_geografica_id AS unidade_geografica_id,
    	ceps_por_municipio.municipio_id_sus AS unidade_geografica_id_sus,
    	internacao.internacao_duracao_id,
    	internacao.internacao_duracao_descricao,
    	internacao.internacao_duracao_ordem,
    	internacao.desfecho_grupo_descricao,
        {{ classificar_coluna_binaria(
            coluna="internacao.atendimento_raps_6m_antes",
        ) }} AS atendimento_raps_6m_antes,
        {{ classificar_coluna_binaria(
            coluna="internacao.atendimento_raps_1m_apos",
        ) }} AS atendimento_raps_1m_apos,
        {{ classificar_coluna_binaria(
            coluna="internacao.menos_6m_ultima_internacao",
        ) }} AS menos_6m_ultima_internacao,
    	cep.latitude AS usuario_residencia_cep_latitude,
    	cep.longitude AS usuario_residencia_cep_longitude,
    	(
    	   cep.latitude::text || ',' || cep.longitude::TEXT
    	) AS usuario_residencia_cep_latlong,
        coalesce(
            cep.bairro_nome,
            'Sem informação'
        ) AS usuario_residencia_bairro,
        internacao.aih_data_inicio AS internacao_data_inicio,
        internacao.condicao_saude_mental_classificao,
        internacao.usuario_residencia_cep,
        internacao.atualizacao_data
    FROM internacoes_provavel_mesmo_usuario internacao
    INNER JOIN listas_de_codigos.ceps_por_municipio
    ON internacao.usuario_residencia_cep = ceps_por_municipio.id_cep
    LEFT JOIN listas_de_codigos.ceps cep
    ON ceps_por_municipio.id_cep = cep.id_cep
),
final AS (
    SELECT
        internacao.id,
        internacao.unidade_geografica_id,
        internacao.unidade_geografica_id_sus,
    	internacao.internacao_duracao_id,
    	internacao.internacao_duracao_descricao,
    	internacao.internacao_duracao_ordem,
        internacao.desfecho_grupo_descricao,
        internacao.atendimento_raps_6m_antes,
        internacao.atendimento_raps_1m_apos,
        internacao.menos_6m_ultima_internacao,
        internacao.usuario_residencia_cep_latitude,
        internacao.usuario_residencia_cep_longitude,
        internacao.usuario_residencia_cep_latlong,
        internacao.usuario_residencia_bairro,
        internacao.internacao_data_inicio,
        internacao.condicao_saude_mental_classificao,
        ceps_por_bairro.latitude AS usuario_residencia_bairro_latitude,
        ceps_por_bairro.longitude AS usuario_residencia_bairro_longitude,
        (
            ceps_por_bairro.latitude::text || ',' || ceps_por_bairro.longitude::text
        ) AS usuario_residencia_bairro_latlong,
        internacao.atualizacao_data
    FROM internacoes_com_cep internacao
    LEFT JOIN ceps_por_bairro
    ON  
        internacao.unidade_geografica_id = ceps_por_bairro.unidade_geografica_id
    AND internacao.unidade_geografica_id_sus = ceps_por_bairro.municipio_id_sus
    AND internacao.usuario_residencia_bairro = ceps_por_bairro.bairro_nome
    AND internacao.usuario_residencia_cep = ANY(ceps_por_bairro.ceps)
)
SELECT * FROM final
