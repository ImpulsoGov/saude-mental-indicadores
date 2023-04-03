{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
encaminhamentos_aps_especializada AS (
    SELECT * FROM {{ ref('encaminhamentos_aps_especializada') }}
),
relacao_aps_especializada AS (
SELECT
    DISTINCT ON (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        conduta
    )
    *
FROM saude_mental.encaminhamentos_aps_especializada
ORDER BY 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    conduta,
    competencia DESC
),
relacao_aps_especializada_horizontalizado AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes,
        sum(quantidade_registrada) FILTER (
            WHERE conduta = 'Encaminhamento para serviço especializado'
        ) AS encaminhamentos_especializada,
        sum(quantidade_registrada_anterior) FILTER (
            WHERE conduta = 'Encaminhamento para serviço especializado'
        ) AS encaminhamentos_especializada_anterior,
        sum(quantidade_registrada) FILTER (
            WHERE conduta = 'Todas'
        ) AS atendimentos_sm_aps,
        sum(quantidade_registrada_anterior) FILTER (
            WHERE conduta = 'Todas'
        ) AS atendimentos_sm_aps_anterior
    FROM relacao_aps_especializada
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes,
        encaminhamentos_especializada,
        atendimentos_sm_aps,
        round(
            100 * encaminhamentos_especializada
            / nullif(atendimentos_sm_aps, 0),
            2
        ) AS perc_encaminhamentos_especializada,
        (
            encaminhamentos_especializada - encaminhamentos_especializada_anterior
        ) AS dif_encaminhamentos_especializada_anterior
    FROM relacao_aps_especializada_horizontalizado
)
SELECT * FROM final