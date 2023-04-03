{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimentos_disseminacao AS (
    SELECT * FROM {{ ref("procedimentos_disseminacao_municipios_selecionados") }}
),
_procedimentos_por_ocupacao_por_mes AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        periodo_id,
        estabelecimento_id_scnes,
        profissional_vinculo_ocupacao_id_cbo2002 AS ocupacao_id_cbo2002,
        sum(quantidade_apresentada) FILTER (
            WHERE instrumento_registro_id_siasus IN ('A', 'B')
        ) AS procedimentos_registrados_raas,
        sum(quantidade_apresentada) FILTER (
            WHERE instrumento_registro_id_siasus IN ('C', 'I')
        ) AS procedimentos_registrados_bpa
    FROM procedimentos_disseminacao
    WHERE estabelecimento_tipo_id_sigtap = '70'  -- CAPS
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio,
        periodo_id,
        estabelecimento_id_scnes,
        profissional_vinculo_ocupacao_id_cbo2002
)
SELECT * FROM _procedimentos_por_ocupacao_por_mes
