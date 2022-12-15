{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
aih_rd_disseminacao AS (
    SELECT * FROM {{ source('sihsus', 'aih_rd_disseminacao') }}
),
final AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        periodo_id,
        periodo_data_inicio
    FROM aih_rd_disseminacao
    ORDER BY
        unidade_geografica_id,
        periodo_data_inicio DESC
)
SELECT * FROM final
