{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
aih_rd AS (
    SELECT * FROM {{ ref("aih_rd_disseminacao_municipios_selecionados") }}
),
periodos AS (
    SELECT * FROM {{ source('codigos', 'periodos') }}
),
preparacao AS (
    SELECT 
        aih_rd.unidade_geografica_id,
        aih_rd.periodo_id,
        periodos.data_inicio AS periodo_data_inicio,
        periodos.id
    FROM aih_rd
    LEFT JOIN periodos
    ON
        aih_rd.periodo_id = periodos.id
    AND periodos.tipo = 'Mensal'
),
final AS (
    SELECT DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        periodo_id,
        periodo_data_inicio
    FROM preparacao
    ORDER BY
        unidade_geografica_id,
        periodo_data_inicio ASC
)
SELECT * FROM final