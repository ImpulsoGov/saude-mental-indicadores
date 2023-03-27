{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimentos_ambulatoriais AS (
    SELECT * FROM {{ ref("procedimentos_disseminacao_municipios_selecionados") }}
),
caps_procedimentos_total AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio AS competencia,
        periodo_id,
        estabelecimento_id_scnes,
        coalesce(
            sum(quantidade_apresentada), 0
        ) AS quantidade_registrada,
        coalesce(
            sum(quantidade_apresentada) FILTER (
                WHERE procedimento_id_sigtap IN (
                    '0301080232',  -- ACOLHIMENTO INICIAL POR CAPS
                    '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (ACOLHIM DEMANDA ESPONT)
                ) 
            ), 0
        ) AS quantidade_registrada_acolhimentos_iniciais_caps
    FROM procedimentos_ambulatoriais    
    WHERE estabelecimento_tipo_id_sigtap = '70' -- CAPS
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio,
        periodo_id,
        estabelecimento_id_scnes
)
SELECT * FROM caps_procedimentos_total