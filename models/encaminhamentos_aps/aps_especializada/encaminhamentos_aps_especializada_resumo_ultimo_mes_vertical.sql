{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
encaminhamentos_aps_especializada_horizontal AS (
    SELECT * FROM {{ ref('encaminhamentos_aps_especializada_resumo_ultimo_mes_horizontal') }}
),
view_vertical AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes,
        'Sim' AS encaminhamento,
        (perc_encaminhamentos_especializada) / 100 AS prop_atendimentos
    FROM encaminhamentos_aps_especializada_horizontal
    UNION
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes,
        'Não' AS encaminhado,
        (100 - perc_encaminhamentos_especializada) / 100 AS prop_atendimentos
    FROM encaminhamentos_aps_especializada_horizontal
),
final AS (
    SELECT
    {{ dbt_utils.surrogate_key([
        "unidade_geografica_id",
        "periodo_id",
        "encaminhamento"
    ]) }} AS id,
    *,
    now() AS atualizacao_data
    FROM view_vertical
)
SELECT * FROM final