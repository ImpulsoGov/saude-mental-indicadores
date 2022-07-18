{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH 
ocupacoes AS (
    SELECT * FROM {{ source('codigos', 'ocupacoes') }}
),
periodos_sucessao AS (
    SELECT * FROM {{ ref('periodos_sucessao') }}
),
vinculos_profissionais AS (
    SELECT *
    FROM {{ source('scnes', 'vinculos_profissionais') }}
),
disponibilidade_por_dia_util AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_cnes,
        ocupacao_id_cbo,
        sum(
            atendimento_carga_ambulatorial
            + atendimento_carga_outras
        ) / 5 AS horas_disponibilidade_diaria
    FROM vinculos_profissionais
    WHERE atendimento_sus
    GROUP BY 
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_cnes,
        ocupacao_id_cbo
),
final AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_cnes,
        ocupacao_id_cbo,
        (
            horas_disponibilidade_diaria
            * {{ schema }}.diferenca_dias_uteis(
                periodos_sucessao.periodo_data_inicio,
                periodos_sucessao.proximo_periodo_data_inicio
            )
        ) AS disponibilidade_mensal
    FROM disponibilidade_por_dia_util
    LEFT JOIN periodos_sucessao
    USING ( periodo_id )
)
SELECT * FROM final
