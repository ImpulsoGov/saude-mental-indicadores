{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

WITH
raas_psicossocial_disseminacao AS (
    SELECT * FROM {{ ref("raas_psicossocial_disseminacao_municipios_selecionados") }}
),

habilitacoes AS (
	SELECT DISTINCT ON (estabelecimento_id_scnes, periodo_id)
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes
    FROM {{ ref("estabelecimentos_habilitacoes_municipios_selecionados") }}
    WHERE habilitacao_id_scnes IN (
        '0616',	-- CAPS I	
        '0617',	-- CAPS II	
        '0618',	-- CAPS III	
        '0619',	-- CAPS ALCOOL E DROGAS	
        '0620',	-- CAPS INFANTIL
        '0635',	-- CAPS AD III
        '0637',	-- CAPS AD IV
        '0621', -- SERVICO HOSPITALAR DE REFERENCIA PARA A ATENCAO INTEGRAL AOS USUARIOS DE ALCOOL E OUTRAS DROGAS
        '0636'	-- SERVIÇOS HOSPITALARES DE REFERENCIA PARA ATENCAO A PESSOAS COM SOFRIMENTO OU TRANTORNO MENTAL INCLUINDO AQUELAS COM NECESSIDADES DECORRENTES DO USO DE ALCOOL E OUTRAS DROGAS
    )
),

raas_distintos AS (
    SELECT DISTINCT
        estabelecimento_id_scnes,
        unidade_geografica_id_sus
    FROM raas_psicossocial_disseminacao
),

habilitacoes_caps_com_registros_sem_todos AS (
    SELECT h.*
    FROM habilitacoes h 
    INNER JOIN raas_distintos rd
    ON h.estabelecimento_id_scnes = rd.estabelecimento_id_scnes
),

habilitacoes_estabelecimento_apenas_todos AS (
    SELECT DISTINCT ON (unidade_geografica_id_sus, periodo_data_inicio)
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        '0000000' AS estabelecimento_id_scnes 
FROM habilitacoes_caps_com_registros_sem_todos
),

habilitacoes_caps_com_registros AS (
    SELECT * FROM habilitacoes_caps_com_registros_sem_todos
    UNION ALL
    SELECT * FROM habilitacoes_estabelecimento_apenas_todos
),

ausentes_ativos AS (	
	SELECT 
        hr.*,
        'caps_usuarios_ativos_perfil_condicao_semsubtotais' AS tabela_referencia
	FROM habilitacoes_caps_com_registros hr
	LEFT JOIN (
        SELECT DISTINCT ON (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
            unidade_geografica_id_sus, 
            estabelecimento_id_scnes, 
            periodo_id
        FROM {{ ref("_caps_usuarios_ativos_perfil_condicao_semsubtotais") }}
    ) ref	
	USING (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
	WHERE ref.estabelecimento_id_scnes IS NULL AND ref.periodo_id IS NULL 
), 

ausentes_novos AS (
	SELECT 
        hr.*,
        'caps_usuarios_novos_perfil_condicao' AS tabela_referencia
	FROM habilitacoes_caps_com_registros hr
	LEFT JOIN (
        SELECT DISTINCT ON (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
            unidade_geografica_id_sus, 
            estabelecimento_id_scnes, 
            periodo_id
        FROM {{ ref("_caps_usuarios_novos_perfil_condicao_semsubtotais") }}
    ) ref	
	USING (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
	WHERE ref.estabelecimento_id_scnes IS NULL AND ref.periodo_id IS NULL 
),

ausentes_atendimentos_individuais AS (	
	SELECT 
        hr.*,
        'caps_usuarios_atendimentos_individuais_perfil_cid' AS tabela_referencia
	FROM habilitacoes_caps_com_registros hr
	LEFT JOIN (
        SELECT DISTINCT ON (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
            unidade_geografica_id_sus, 
            estabelecimento_id_scnes, 
            periodo_id
        FROM {{ ref("_caps_usuarios_atendimentos_individuais_perfil_cid") }}
    ) ref	
	USING (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
	WHERE ref.estabelecimento_id_scnes IS NULL AND ref.periodo_id IS NULL 
), 

ausentes_adesao_mensal AS (	
	SELECT 
        hr.*,
        'caps_adesao_usuarios_perfil_cid' AS tabela_referencia
	FROM habilitacoes_caps_com_registros hr
	LEFT JOIN (
        SELECT DISTINCT ON (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
            unidade_geografica_id_sus, 
            estabelecimento_id_scnes, 
            periodo_id
        FROM {{ ref("_caps_adesao_usuarios_perfil_cid") }}
    ) ref	
	USING (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
	WHERE ref.estabelecimento_id_scnes IS NULL 
        AND ref.periodo_id IS NULL 
        AND hr.periodo_data_inicio < (
            SELECT DATE_TRUNC('month', MAX(periodo_data_inicio)) - INTERVAL '3 months'
            FROM habilitacoes_caps_com_registros
    )
), 

ausentes_adesao_acumulada AS (	
	SELECT 
        hr.*,
        'caps_adesao_evasao_coortes_resumo' AS tabela_referencia
	FROM habilitacoes_caps_com_registros hr
	LEFT JOIN (
        SELECT DISTINCT ON (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
            unidade_geografica_id_sus, 
            estabelecimento_id_scnes, 
            periodo_id
        FROM {{ ref("_caps_adesao_evasao_coortes_resumo") }}
    ) ref	
	USING (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
	WHERE ref.estabelecimento_id_scnes IS NULL 
        AND ref.periodo_id IS NULL 
        AND hr.periodo_data_inicio < (
            SELECT DATE_TRUNC('month', MAX(periodo_data_inicio)) - INTERVAL '3 months'
            FROM habilitacoes_caps_com_registros
    )
), 

ausentes_proced_por_usuario AS (	
	SELECT 
        hr.*,
        'caps_procedimentos_por_usuario_por_tempo_servico' AS tabela_referencia
	FROM habilitacoes_caps_com_registros hr
	LEFT JOIN (
        SELECT DISTINCT ON (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
            unidade_geografica_id_sus, 
            estabelecimento_id_scnes, 
            periodo_id
        FROM {{ ref("_caps_procedimentos_por_usuario_por_tempo_servico") }}
    ) ref	
	USING (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
	WHERE ref.estabelecimento_id_scnes IS NULL AND ref.periodo_id IS NULL 
), 

ausentes_procedimentos_hora AS (	
	SELECT 
        hr.*,
        'caps_procedimentos_por_hora_resumo' AS tabela_referencia
	FROM habilitacoes_caps_com_registros hr
	LEFT JOIN (
        SELECT DISTINCT ON (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
            unidade_geografica_id_sus, 
            estabelecimento_id_scnes, 
            periodo_id
        FROM {{ ref("_caps_procedimentos_por_hora_resumo") }}
    ) ref	
	USING (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
	WHERE ref.estabelecimento_id_scnes IS NULL AND ref.periodo_id IS NULL 
), 

ausentes_procedimentos_tipo AS (	
	SELECT 
        hr.*,
        'caps_procedimentos_por_tipo' AS tabela_referencia
	FROM habilitacoes_caps_com_registros hr
	LEFT JOIN (
        SELECT DISTINCT ON (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
            unidade_geografica_id_sus, 
            estabelecimento_id_scnes, 
            periodo_id
        FROM {{ ref("_caps_procedimentos_por_tipo") }}
    ) ref	
	USING (unidade_geografica_id_sus, estabelecimento_id_scnes, periodo_id) 
	WHERE ref.estabelecimento_id_scnes IS NULL AND ref.periodo_id IS NULL 
), 

ausentes_todos AS (
    SELECT * FROM ausentes_ativos
    UNION ALL
    SELECT * FROM ausentes_novos
    UNION ALL
    SELECT * FROM ausentes_atendimentos_individuais
    UNION ALL
    SELECT * FROM ausentes_adesao_mensal
    UNION ALL
    SELECT * FROM ausentes_adesao_acumulada
    UNION ALL
    SELECT * FROM ausentes_proced_por_usuario
    UNION ALL
    SELECT * FROM ausentes_procedimentos_hora
    UNION ALL
    SELECT * FROM ausentes_procedimentos_tipo
),

final AS (    
    SELECT
		{{ dbt_utils.surrogate_key([
			"unidade_geografica_id",
			"unidade_geografica_id_sus",
            "periodo_id",
            "estabelecimento_id_scnes",
            "tabela_referencia"
		]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        now() AS atualizacao_data,
        tabela_referencia
    FROM ausentes_todos
)

SELECT * FROM final
