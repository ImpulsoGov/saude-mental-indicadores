
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
vinculos_profissionais AS (
	SELECT 
        estabelecimento_id_scnes,
        estabelecimento_municipio_id_sus,
        -- estabelecimento_regiao_saude_id_sus,
        -- estabelecimento_microrregiao_saude_id_sus,
        -- estabelecimento_distrito_sanitario_id_sus,
        -- estabelecimento_distrito_administrativo_id_sus,
        -- estabelecimento_gestao_condicao_id_scnes,
        -- estabelecimento_personalidade_juridica_id_scnes,
        -- estabelecimento_id_cpf_cnpj,
        -- estabelecimento_mantido,
        -- estabelecimento_mantenedora_id_cnpj,
        -- estabelecimento_esfera_id_scnes,
        -- estabelecimento_atividade_ensino_id_scnes,
        -- estabelecimento_tributos_retencao_id_scnes,
        estabelecimento_natureza_id_scnes,
        -- estabelecimento_fluxo_id_scnes,
        -- estabelecimento_tipo_id_scnes,
        -- estabelecimento_turno_id_scnes,
        -- estabelecimento_hierarquia_id_scnes,
        -- estabelecimento_terceiro,
        profissional_id_cpf_criptografado,
        profissional_cpf_unico,
        ocupacao_id_cbo2002,
        ocupacao_cbo_unico,
        profissional_nome,
        profissional_id_cns,
        -- profissional_conselho_tipo_id_scnes,
        -- profissional_id_conselho,
        tipo_id_scnes,
        contratado,
        autonomo,
        sem_vinculo_definido,
        atendimento_sus,
        atendimento_nao_sus,
        atendimento_carga_outras,
        atendimento_carga_hospitalar,
        atendimento_carga_ambulatorial,
        periodo_data_inicio,
        -- profissional_residencia_municipio_id_sus,
        estabelecimento_natureza_juridica_id_scnes,
        id,
        periodo_id,
        unidade_geografica_id,
        criacao_data,
        atualizacao_data
    FROM {{ source("scnes", "vinculos_profissionais") }}
    WHERE estabelecimento_municipio_id_sus IN 
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
SELECT * FROM vinculos_profissionais
