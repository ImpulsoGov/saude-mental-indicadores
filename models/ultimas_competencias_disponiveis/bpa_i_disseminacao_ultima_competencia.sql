{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
bpa_i_disseminacao AS (
    SELECT * FROM {{ ref("bpa_i_disseminacao_municipios_selecionados") }}
),
final AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        periodo_id,
        processamento_periodo_data_inicio AS periodo_data_inicio
    FROM bpa_i_disseminacao
    ORDER BY
        unidade_geografica_id,
        processamento_periodo_data_inicio DESC
)
SELECT * FROM final
