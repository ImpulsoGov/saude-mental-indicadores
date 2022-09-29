{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
sisab_producao_municipios_por_tipo_equipe_por_tipo_producao AS (
    SELECT * FROM {{ source('sisab', 'sisab_producao_municipios_por_tipo_equipe_por_tipo_producao') }}
        WHERE
        tipo_equipe = 'Eq. Consultório na Rua - ECR'
    AND tipo_producao = 'Atendimento Individual'
),
periodos AS (
    SELECT * FROM {{ source('codigos', 'periodos') }}
),
unidades_geograficas AS (
    SELECT * FROM {{ source('codigos', 'unidades_geograficas') }}
),
preparacao AS (
    SELECT
        unidade_geografica_id,
        periodo_id
    FROM sisab_producao_municipios_por_tipo_equipe_por_tipo_producao
    GROUP BY
        unidade_geografica_id,
        periodo_id
        ),
final AS (
    SELECT
        preparacao.unidade_geografica_id,
        periodos.data_inicio AS periodo_data_inicio,
        periodos.id AS periodo_id

    FROM preparacao
    LEFT JOIN periodos
    ON 
        preparacao.periodo_id = periodos.id
    AND periodos.tipo = 'Mensal'
    LEFT JOIN listas_de_codigos.unidades_geograficas unidade_geografica
    ON preparacao.unidade_geografica_id = unidade_geografica.id
)
SELECT * FROM final