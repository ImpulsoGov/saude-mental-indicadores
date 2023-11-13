
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
bpa_i_disseminacao AS (
	SELECT 
        id,
        estabelecimento_id_scnes,
        -- gestao_unidade_geografica_id_sus,
        -- gestao_condicao_id_siasus,
        unidade_geografica_id_sus,
        estabelecimento_tipo_id_sigtap,
        -- prestador_tipo_id_sigtap,
        -- estabelecimento_mantido,
        -- estabelecimento_id_cnpj,
        -- mantenedora_id_cnpj,
        processamento_periodo_data_inicio,
        realizacao_periodo_data_inicio,
        procedimento_id_sigtap,
        -- financiamento_tipo_id_sigtap,
        -- financiamento_subtipo_id_sigtap,
        -- complexidade_id_siasus,
        -- autorizacao_id_siasus,
        profissional_id_cns,
        profissional_vinculo_ocupacao_id_cbo2002,
        condicao_principal_id_cid10,
        carater_atendimento_id_siasus,
        usuario_id_cns_criptografado,
        usuario_nascimento_data,
        usuario_idade_tipo_id_sigtap,
        usuario_idade,
        usuario_sexo_id_sigtap,
        usuario_raca_cor_id_siasus,
        usuario_residencia_municipio_id_sus,
        quantidade_apresentada,
        quantidade_aprovada,
        valor_apresentado,
        valor_aprovado,
        -- atendimento_residencia_ufs_distintas,
        -- atendimento_residencia_municipios_distintos,
        -- receptor_credito_id_cnpj,
        usuario_etnia_id_sus,
        -- estabelecimento_natureza_juridica_id_scnes,
        unidade_geografica_id,
        periodo_id,
        criacao_data,
        atualizacao_data 
    FROM {{ source("siasus", "bpa_i_disseminacao") }}
    WHERE unidade_geografica_id_sus IN 
        (
            '150140', -- Belém/PA
            '230190', -- Barbalha/CE
            '230440', -- Fortaleza/CE (para Impulsolandia)
            '231290', -- Sobral/CE
            '261160', -- Recife/PE
            '280030', -- Aracaju/SE
            '292740', -- Salvador/BA
            '315780', -- Santa Luzia/MG
            '320500', -- Serra/ES
            '320520', -- Vila Velha/ES  
            -- '330490', -- São Gonçalo/RJ
            -- '350950', -- Campinas/SP
            '351640', -- Franco da Rocha/SP
            '352590', -- Jundiaí/SP
            -- '410480', -- Cascavel/PR
            -- '410690', -- Curitiba/PR
            '431440', -- Pelotas/RS
            '431490', -- Porto Alegre/RS   
            '520140',  -- Aparecida de Goiânia/GO 

-- MUNICIPIOS BNDES
            -- '260120', -- Arcoverde/PE
            -- '210005', -- Açailândia/MA
            -- '150170', -- Bragança/PA
            -- '230280', -- Canindé/CE
            -- '280290', -- Itabaiana/SE
            -- '280350', -- Lagarto/SE
            -- '160030', -- Macapá/AP
            -- '280480', -- Nossa Senhora do Socorro/SE
            -- '230970', -- Pacatuba/CE
            '231130', -- Quixadá/CE
            -- '231140', -- Quixeramobim/CE
            '150680' -- Santarém/PA
            -- '261390', -- Serra Talhada/PE           
        )
)
SELECT * FROM bpa_i_disseminacao
