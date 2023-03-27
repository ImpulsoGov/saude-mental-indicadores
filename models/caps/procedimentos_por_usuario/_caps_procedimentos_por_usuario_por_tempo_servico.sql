{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimentos_usuario_mes AS (
    SELECT * FROM {{ ref("_caps_procedimentos_por_usuario_por_mes") }}
),
por_tempo_servico_por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        competencia,
        periodo_id,
        tempo_servico_descricao,
        round(
            (
                sum(procedimentos_exceto_acolhimento)::numeric
                / nullif(count(DISTINCT usuario_id_cns_criptografado), 0)
            ),
            1
        ) AS procedimentos_por_usuario
    FROM procedimentos_usuario_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        competencia,
        periodo_id,
        tempo_servico_descricao
),
por_tempo_servico_todos_estabelecimentos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        '0000000' AS estabelecimento_id_scnes,
        competencia,
        periodo_id,
        tempo_servico_descricao,
        round(
            (
                sum(procedimentos_exceto_acolhimento)::numeric
                / nullif(count(DISTINCT usuario_id_cns_criptografado), 0)
            ),
            1
        ) AS procedimentos_por_usuario
    FROM procedimentos_usuario_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        tempo_servico_descricao
),
por_tempo_servico AS (
    SELECT * FROM por_tempo_servico_por_estabelecimento
    UNION
    SELECT * FROM por_tempo_servico_todos_estabelecimentos
),
final AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        estabelecimento_id_scnes,
        competencia AS periodo_data_inicio,
        tempo_servico_descricao,
        procedimentos_por_usuario
    FROM por_tempo_servico
)
SELECT * FROM final