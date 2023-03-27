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
condicoes_saude AS (
    SELECT 
        DISTINCT ON (id_cid10)
        *
    FROM {{ source("codigos", "condicoes_saude") }}
    ORDER BY id_cid10
),
procedimentos_caps AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        usuario_nascimento_data,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        procedimento_id_sigtap NOT IN (
        -- lista de procedimentos BPA-i NÃO considerados atend individual
            '0301080321',  -- Acompanhamento de SRT por CAPS
            -- procedimentos lançados indevidamente como BPA-i
            '0101050011',  -- Praticas corporais em med tradicional chinesa 
            '0101050020',  -- Terapia comunitária
            '0301050023',  -- Assist domiciliar por equipe multiprofissional
            '0101050135',  -- Sessão de dança circular
            '0101050054',  -- Oficina de massagem/ auto-massagem
            '0101050089',  -- Sessão de musicoterapia
            '0301080143',  -- Atendimento em oficina terapeutica
            '0101050062',  -- Sessão de arteterapia
            '0101050070'   -- Sessão de meditação
        ) AS atendimento_individual,
        quantidade_apresentada
    FROM bpa_i_disseminacao
    WHERE 
        estabelecimento_tipo_id_sigtap = '70'  -- CAPS
        -- ignorar acolhimentos iniciais
        AND procedimento_id_sigtap NOT IN (
            '0301080232',  -- ACOLHIMENTO INICIAL POR CAPS
            '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (ACOLHIM DEMANDA ESPONT)
        ) 
    UNION
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        usuario_nascimento_data,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        -- ATENDIMENTO INDIVIDUAL DE PACIENTE EM CAPS
        procedimento_id_sigtap = '0301080208' AS atendimento_individual,
        quantidade_apresentada
    FROM raas_psicossocial_disseminacao
),
{{ classificar_faixa_etaria(
    relacao="procedimentos_caps",
    coluna_nascimento_data="usuario_nascimento_data",
    coluna_data_referencia="realizacao_periodo_data_inicio",
    idade_tipo="Anos",
    faixa_etaria_agrupamento="10 em 10 anos",
    colunas_faixa_etaria=["id", "descricao", "ordem"],
    cte_resultado="procedimentos_com_idade_usuario"
) }},
usuarios_atendimentos_individuais AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio AS competencia,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        faixa_etaria_id AS usuario_faixa_etaria_id,
        faixa_etaria_descricao AS usuario_faixa_etaria_descricao,
        faixa_etaria_ordem AS usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        condicoes_saude.grupo_descricao_curta_cid10,
        usuario_raca_cor_id_siasus,
        coalesce(
            sum(quantidade_apresentada) FILTER (WHERE NOT atendimento_individual),
            0
        ) > 0 AS procedimentos_alem_individual,
        (sum(quantidade_apresentada) > 0)::bool AS fez_procedimentos
    FROM procedimentos_com_idade_usuario
    -- TODO: usar tabela de CIDs do schema `listas_de_codigos`
    LEFT JOIN condicoes_saude
            ON procedimentos_com_idade_usuario.condicao_principal_id_cid10 = 
            condicoes_saude.id_cid10
    -- TODO: adicionar procedimentos registrados em BPAi
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        faixa_etaria_id, 
        faixa_etaria_descricao,
        faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        grupo_descricao_curta_cid10,
        usuario_raca_cor_id_siasus
)
SELECT * FROM usuarios_atendimentos_individuais