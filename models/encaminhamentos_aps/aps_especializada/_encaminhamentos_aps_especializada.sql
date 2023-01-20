{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
sisab_producao_por_conduta AS (
    SELECT * FROM {{ source('sisab', 'sisab_producao_conduta_problema_condicao') }}
),
saude_mental_encaminhamentos_especializada AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        sum(quantidade_registrada) AS quantidade_registrada,
        'Encaminhamento para serviço especializado' AS conduta
    FROM sisab_producao_por_conduta
    WHERE 
        conduta  = 'Encaminhamento p/ serviço especializado'
    AND problema_condicao_avaliada 
        = ANY (ARRAY[
            'Saúde mental'::text,
            'Usuário de álcool'::text,
            'Usuário de outras drogas'::TEXT
        ])
    GROUP BY 
        unidade_geografica_id,
        periodo_id
),
saude_mental_todas_condutas AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        sum(quantidade_registrada) AS quantidade_registrada,
        'Todas' AS conduta
    FROM sisab_producao_por_conduta
    WHERE problema_condicao_avaliada = ANY (ARRAY[
        'Saúde mental',
        'Usuário de álcool',
        'Usuário de outras drogas'
    ])
    GROUP BY 
        unidade_geografica_id,
        periodo_id
),
saude_mental_condutas AS (
    SELECT * FROM saude_mental_encaminhamentos_especializada
    UNION
    SELECT * FROM saude_mental_todas_condutas
)