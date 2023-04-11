{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
vinculos_profissionais AS (
    SELECT * FROM {{ ref('vinculos_profissionais_municipios_selecionados') }}
),
final AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        periodo_id,
        periodo_data_inicio AS periodo_data_inicio
    FROM vinculos_profissionais
    ORDER BY
        unidade_geografica_id,
        periodo_data_inicio DESC
)
SELECT * FROM final
