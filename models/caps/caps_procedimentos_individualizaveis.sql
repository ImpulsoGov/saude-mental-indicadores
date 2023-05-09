{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
raas_psicossocial_disseminacao AS (
    SELECT * FROM {{ ref("raas_psicossocial_disseminacao_municipios_selecionados") }}
),
bpa_i_disseminacao AS (
    SELECT * FROM {{ ref("bpa_i_disseminacao_municipios_selecionados") }}
),
procedimentos_caps_raas AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        estabelecimento_tipo_id_sigtap,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        usuario_residencia_municipio_id_sus,
        usuario_nascimento_data,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias,
        carater_atendimento_id_siasus,
        procedimento_id_sigtap,
        quantidade_apresentada
    FROM raas_psicossocial_disseminacao
    WHERE quantidade_apresentada > 0
    {%- if is_incremental() %}
    -- TODO: suporte completo a atualizações retroativas
    AND criacao_data > (SELECT max(atualizacao_data) FROM {{ this }})
    {%- endif %}
),
procedimentos_caps_bpa_i AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        estabelecimento_tipo_id_sigtap,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        usuario_residencia_municipio_id_sus,
        usuario_nascimento_data,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        NULL::bool AS usuario_situacao_rua,
        NULL::bool AS usuario_abuso_substancias,
        carater_atendimento_id_siasus,
        procedimento_id_sigtap,
        quantidade_apresentada
    FROM bpa_i_disseminacao
    WHERE 
        estabelecimento_tipo_id_sigtap = '70'  -- CAPS
        -- ou id_scnes bate com uma tabela de exceções de coisas que são CAPS
    AND quantidade_apresentada > 0
    {%- if is_incremental() %}
    -- TODO: suporte completo a atualizações retroativas
    AND criacao_data > (SELECT max(atualizacao_data) FROM {{ this }})
    {%- endif %}
),
final AS (
    SELECT
        *,
        now() AS atualizacao_data
    FROM procedimentos_caps_raas
    UNION
    SELECT
        *,
        now() AS atualizacao_data
    FROM procedimentos_caps_bpa_i
)
SELECT * FROM final
