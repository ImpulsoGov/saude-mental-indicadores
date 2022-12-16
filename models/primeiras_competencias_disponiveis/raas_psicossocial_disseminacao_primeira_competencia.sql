{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
raas_psicossocial_disseminacao AS (
    SELECT * FROM {{ source("siasus", "raas_psicossocial_disseminacao") }}
),
final AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        periodo_id,
        processamento_periodo_data_inicio AS periodo_data_inicio
    FROM raas_psicossocial_disseminacao
    ORDER BY
        unidade_geografica_id,
        processamento_periodo_data_inicio ASC
)
SELECT * FROM final
