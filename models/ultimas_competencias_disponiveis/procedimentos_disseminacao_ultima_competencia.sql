{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
procedimentos_disseminacao AS (
    SELECT * FROM {{ ref("procedimentos_disseminacao_municipios_selecionados") }}
),
final AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        periodo_id,
        realizacao_periodo_data_inicio AS periodo_data_inicio
    FROM procedimentos_disseminacao
    ORDER BY
        unidade_geografica_id,
        realizacao_periodo_data_inicio DESC
)
SELECT * FROM final