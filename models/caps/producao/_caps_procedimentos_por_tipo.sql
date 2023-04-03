{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimentos_disseminacao AS (
    SELECT * FROM {{ ref("procedimentos_disseminacao_municipios_selecionados") }}
),
procedimentos_sigtap AS (
    SELECT * FROM {{ source("codigos", "procedimentos_sigtap") }}
),
procedimentos_por_tipo AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        periodo_id,
        estabelecimento_id_scnes,
        procedimento_id_sigtap,
        coalesce(
            sum(quantidade_apresentada) FILTER (
                WHERE instrumento_registro_id_siasus IN ('A', 'B')
            ),
            0
        ) AS procedimentos_registrados_raas,
        coalesce(
            sum(quantidade_apresentada) FILTER (
                WHERE instrumento_registro_id_siasus IN ('C', 'I')
            ),
            0
        ) AS procedimentos_registrados_bpa
    FROM procedimentos_disseminacao
    WHERE estabelecimento_tipo_id_sigtap = '70'  -- CAPS
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio,
        periodo_id,
        estabelecimento_id_scnes,
        procedimento_id_sigtap
),
final AS (
    SELECT  
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "estabelecimento_id_scnes",
            "periodo_id",
            "procedimento_id_sigtap"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_data_inicio,
        periodo_id,
        estabelecimento_id_scnes,
        procedimentos_sigtap.procedimento_nome AS procedimento,
        procedimentos_registrados_raas,
        procedimentos_registrados_bpa,
        (
            procedimentos_registrados_raas + procedimentos_registrados_bpa
        ) AS procedimentos_registrados_total
    FROM procedimentos_por_tipo
    LEFT JOIN procedimentos_sigtap
    ON procedimentos_por_tipo.procedimento_id_sigtap = procedimentos_sigtap.procedimento_id
)
SELECT * FROM final
