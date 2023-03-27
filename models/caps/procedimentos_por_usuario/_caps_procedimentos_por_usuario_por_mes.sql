{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
bpa_i_disseminacao AS (
    SELECT * FROM {{ source('siasus', 'bpa_i_disseminacao') }}
),
raas_psicossocial_disseminacao AS (
    SELECT * FROM {{ source('siasus', 'raas_psicossocial_disseminacao') }}
),
ativos_raas AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio AS competencia,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        sum(quantidade_apresentada) AS procedimentos_registrados_raas
    FROM raas_psicossocial_disseminacao
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado
),
ativos_bpa_i AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio AS competencia,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        sum(quantidade_apresentada) AS procedimentos_registrados_bpa_i,
        sum(quantidade_apresentada) FILTER (
            WHERE procedimento_id_sigtap IN (
                '0301080232',  -- ACOLHIMENTO INICIAL POR CAPS
                '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (AC DEMANDA ESPONT)
            )
        ) AS acolhimentos_iniciais_em_caps
    FROM bpa_i_disseminacao
    WHERE estabelecimento_tipo_id_sigtap = '70'  -- CAPS
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado
),
procedimentos_raas_bpa_i AS (
    SELECT
    	coalesce(
    		ativos_raas.unidade_geografica_id,
    		ativos_bpa_i.unidade_geografica_id
    	) AS unidade_geografica_id,
        coalesce(
            ativos_raas.unidade_geografica_id_sus,
            ativos_bpa_i.unidade_geografica_id_sus
        ) AS unidade_geografica_id_sus,
    	coalesce(
    		ativos_raas.periodo_id,
    		ativos_bpa_i.periodo_id
    	) AS periodo_id,
        coalesce(
            ativos_raas.competencia,
            ativos_bpa_i.competencia
        ) AS competencia,
    	coalesce(
    		ativos_raas.estabelecimento_id_scnes,
    		ativos_bpa_i.estabelecimento_id_scnes
    	) AS estabelecimento_id_scnes,
    	coalesce(
    		ativos_raas.usuario_id_cns_criptografado,
    		ativos_bpa_i.usuario_id_cns_criptografado
    	) AS usuario_id_cns_criptografado,
    	coalesce(
    		procedimentos_registrados_raas,
    		0
    	) AS procedimentos_registrados_raas,
    	coalesce(
    		procedimentos_registrados_bpa_i,
    		0
    	) AS procedimentos_registrados_bpa_i,
    	coalesce(
    		acolhimentos_iniciais_em_caps,
    		0
    	) AS acolhimentos_iniciais_em_caps
    FROM ativos_raas
    FULL JOIN  ativos_bpa_i
    ON
    	ativos_raas.unidade_geografica_id = ativos_bpa_i.unidade_geografica_id
    AND ativos_raas.periodo_id = ativos_bpa_i.periodo_id
    AND ativos_raas.estabelecimento_id_scnes
        = ativos_bpa_i.estabelecimento_id_scnes
    AND ativos_raas.usuario_id_cns_criptografado
        = ativos_bpa_i.usuario_id_cns_criptografado
),
usuario_primeiro_procedimento AS (
    SELECT 
        DISTINCT ON (
            unidade_geografica_id,
            unidade_geografica_id_sus,
            estabelecimento_id_scnes,
            usuario_id_cns_criptografado
        )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        competencia AS competencia_primeiro_procedimento
    FROM procedimentos_raas_bpa_i
    ORDER BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        competencia ASC
),
final AS (
    SELECT 
        procedimentos_raas_bpa_i.*,
        (
        procedimentos_registrados_raas 
        + procedimentos_registrados_bpa_i
        - acolhimentos_iniciais_em_caps
        ) AS procedimentos_exceto_acolhimento,
        usuario_primeiro_procedimento.competencia_primeiro_procedimento
    FROM procedimentos_raas_bpa_i
    LEFT JOIN usuario_primeiro_procedimento
    USING (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado
    )
),
{{ classificar_tempo_servico(
    relacao="final",
    coluna_primeiro_procedimento=(
        "competencia_primeiro_procedimento"
    ),
    coluna_data_referencia="competencia",
    cte_resultado="final_com_tempo_servico",
) }}
SELECT * FROM final_com_tempo_servico