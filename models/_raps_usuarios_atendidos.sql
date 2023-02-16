{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- set procedimentos_urgencia -%}
SELECT procedimento_id_sigtap FROM {{ ref("procedimentos_urgencia") }}
{%- endset -%}

WITH
ambulatorios_atendimentos AS (
    SELECT * FROM {{ ref("ambulatorio_atendimentos") }}
),
caps_atendimentos AS (
    SELECT * FROM {{ ref("caps_procedimentos_individualizaveis") }}
),
raps_atendimentos AS (
    SELECT
        periodo_id,
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_tipo_id_sigtap,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        procedimento_id_sigtap,
        carater_atendimento_id_siasus,
        usuario_residencia_municipio_id_sus,
        usuario_nascimento_data,
        condicao_principal_id_cid10,
        usuario_sexo_id_sigtap,
        usuario_raca_cor_id_siasus,
        NULL AS usuario_situacao_rua,
        NULL AS usuario_abuso_substancias,
        quantidade_apresentada,
        atualizacao_data
    FROM ambulatorios_atendimentos
    UNION
    SELECT
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        '70' AS estabelecimento_tipo_id_sigtap,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        procedimento_id_sigtap,
        carater_atendimento_id_siasus,
        usuario_residencia_municipio_id_sus,
        usuario_nascimento_data,
        condicao_principal_id_cid10,
        usuario_sexo_id_sigtap,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias,
        quantidade_apresentada,
        atualizacao_data
    FROM caps_atendimentos
),
atendimentos_por_usuario AS (
    SELECT
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        mode() WITHIN GROUP (
            ORDER BY estabelecimento_tipo_id_sigtap
        ) AS estabelecimento_tipo_id_sigtap,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        mode() WITHIN GROUP (
            ORDER BY usuario_residencia_municipio_id_sus
        ) AS usuario_residencia_municipio_id_sus,
        mode() WITHIN GROUP (
            ORDER BY usuario_nascimento_data
        ) AS usuario_nascimento_data,
        mode() WITHIN GROUP (
            ORDER BY usuario_sexo_id_sigtap
        ) AS usuario_sexo_id_sigtap,
        mode() WITHIN GROUP (
            ORDER BY usuario_raca_cor_id_siasus
        ) AS usuario_raca_cor_id_siasus,
        mode() WITHIN GROUP (
            ORDER BY condicao_principal_id_cid10
        ) AS condicao_principal_id_cid10,
        mode() WITHIN GROUP (
            ORDER BY usuario_situacao_rua
        ) AS usuario_situacao_rua,
        mode() WITHIN GROUP (
            ORDER BY usuario_abuso_substancias
        ) AS usuario_abuso_substancias,
        sum(quantidade_apresentada) FILTER (
            WHERE
                procedimento_id_sigtap NOT IN ({{ procedimentos_urgencia }})
            AND carater_atendimento_id_siasus <> '02'
        ) AS quantidade_registrada_rotina,
        sum(quantidade_apresentada) FILTER (
            WHERE
                procedimento_id_sigtap IN ({{ procedimentos_urgencia }})
            OR carater_atendimento_id_siasus = '02'
        ) AS quantidade_registrada_urgencia,
        max(atualizacao_data) AS atualizacao_data
    FROM raps_atendimentos
    GROUP BY
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado
    {%- if is_incremental() %}
    HAVING max(atualizacao_data) > (
        SELECT max(atualizacao_data)
        FROM {{ this }}
    )
    {%- endif %}
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id",
            "estabelecimento_id_scnes",
            "usuario_id_cns_criptografado"
        ]) }} AS id,
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_tipo_id_sigtap,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        usuario_residencia_municipio_id_sus,
        usuario_nascimento_data,
        usuario_sexo_id_sigtap,
        usuario_raca_cor_id_siasus,
        condicao_principal_id_cid10,
        usuario_situacao_rua,
        usuario_abuso_substancias,
        coalesce(
            quantidade_registrada_rotina,
            0
        ) AS quantidade_registrada_rotina,
        coalesce(
            quantidade_registrada_urgencia,
            0
        ) AS quantidade_registrada_urgencia,
        atualizacao_data
    FROM atendimentos_por_usuario
)
SELECT * FROM final
