{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
sisab_producao_municipios_por_tipo_equipe_por_tipo_producao AS (
    SELECT * FROM {{ source('sisab', 'sisab_producao_municipios_por_tipo_equipe_por_tipo_producao') }}
),
periodos AS (
    SELECT * FROM {{ source('codigos', 'periodos') }}
),
preparacao AS (
    SELECT 
        sisab_producao_municipios_por_tipo_equipe_por_tipo_producao.unidade_geografica_id,
        sisab_producao_municipios_por_tipo_equipe_por_tipo_producao.periodo_id,
        periodos.data_inicio AS periodo_data_inicio,
        periodos.id
    FROM sisab_producao_municipios_por_tipo_equipe_por_tipo_producao
    LEFT JOIN periodos
    ON 
        sisab_producao_municipios_por_tipo_equipe_por_tipo_producao.periodo_id = periodos.id
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
        periodo_data_inicio DESC
)
SELECT * FROM final