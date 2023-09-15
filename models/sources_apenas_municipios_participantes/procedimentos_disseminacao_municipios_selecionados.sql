
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
procedimentos_disseminacao AS (
	SELECT 
        id,
        periodo_id,
        unidade_geografica_id,
        estabelecimento_id_scnes,
        -- gestao_unidade_geografica_id_sus,
        -- gestao_condicao_id_siasus,
        unidade_geografica_id_sus,
        -- regra_contratual_id_scnes,
        -- incremento_outros_id_sigtap,
        -- incremento_urgencia_id_sigtap,
        estabelecimento_tipo_id_sigtap,
        -- prestador_tipo_id_sigtap,
        -- estabelecimento_mantido,
        -- estabelecimento_id_cnpj,
        -- mantenedora_id_cnpj,
        -- receptor_credito_id_cnpj,
        processamento_periodo_data_inicio,
        realizacao_periodo_data_inicio,
        procedimento_id_sigtap,
        -- financiamento_tipo_id_sigtap,
        -- financiamento_subtipo_id_sigtap,
        complexidade_id_siasus,
        autorizacao_id_siasus,
        profissional_id_cns,
        profissional_vinculo_ocupacao_id_cbo2002,
        desfecho_motivo_id_siasus,
        obito,
        encerramento,
        permanencia,
        alta,
        transferencia,
        condicao_principal_id_cid10,
        condicao_secundaria_id_cid10,
        condicao_associada_id_cid10,
        carater_atendimento_id_siasus,
        usuario_idade,
        -- procedimento_idade_minima,
        -- procedimento_idade_maxima,
        -- compatibilidade_idade_id_siasus,
        usuario_sexo_id_sigtap,
        usuario_raca_cor_id_siasus,
        usuario_residencia_municipio_id_sus,
        quantidade_apresentada,
        quantidade_aprovada,
        valor_apresentado,
        valor_aprovado,
        atendimento_residencia_ufs_distintas,
        atendimento_residencia_municipios_distintos,
        -- procedimento_valor_diferenca_sigtap,
        -- procedimento_valor_vpa,
        -- procedimento_valor_sigtap,
        aprovacao_status_id_siasus,
        ocorrencia_id_siasus,
        -- erro_quantidade_apresentada_id_siasus,
        -- erro_apac,
        usuario_etnia_id_sus,
        -- complemento_valor_federal,
        -- complemento_valor_local,
        -- incremento_valor,
        servico_id_sigtap,
        servico_classificacao_id_sigtap,
        equipe_id_ine,
        estabelecimento_natureza_juridica_id_scnes,
        instrumento_registro_id_siasus,
        criacao_data,
        atualizacao_data
    FROM {{ source("siasus", "procedimentos_disseminacao") }}
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
            '520140'  -- Aparecida de Goiânia/GO            
        )
)
SELECT * FROM procedimentos_disseminacao
