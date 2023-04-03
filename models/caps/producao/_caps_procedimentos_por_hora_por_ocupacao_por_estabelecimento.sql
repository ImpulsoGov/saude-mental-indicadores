{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
disponibilidade_ocupacao_por_estabelecimento AS (
    SELECT * FROM {{ ref("disponibilidade_ocupacao_por_estabelecimento") }}
),
procedimentos AS (
    SELECT * FROM {{ ref("_caps_procedimentos_por_ocupacao_por_mes") }}
),

-- Cruzar o número de procedimentos registrados com o número de horas trabalhadas por cada categoria profissional em cada estabelecimento
procedimentos_hora_ocupacao_estabelecimento AS (    
    SELECT
        procedimentos.unidade_geografica_id,
        procedimentos.unidade_geografica_id_sus,
        procedimentos.periodo_data_inicio,
        procedimentos.periodo_id,
        procedimentos.estabelecimento_id_scnes,
        procedimentos.ocupacao_id_cbo2002,
        coalesce(
            procedimentos.procedimentos_registrados_raas,
            0
        ) AS procedimentos_registrados_raas,
        coalesce(
            procedimentos.procedimentos_registrados_bpa,
            0
        ) AS procedimentos_registrados_bpa,
        (
            coalesce(procedimentos_registrados_raas, 0)
            + coalesce(procedimentos_registrados_bpa, 0)
        ) AS procedimentos_registrados_total,
        disponibilidade_ocupacao_por_estabelecimento.disponibilidade_mensal 
            AS horas_disponibilidade_profissionais
    FROM procedimentos
    LEFT JOIN disponibilidade_ocupacao_por_estabelecimento
    USING (
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_scnes,
        ocupacao_id_cbo2002
    )
),
{{  revelar_combinacoes_implicitas(
    relacao="procedimentos_hora_ocupacao_estabelecimento",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus"
    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["estabelecimento_id_scnes"],
        ["ocupacao_id_cbo2002"]
    ],
    cte_resultado="com_combinacoes_vazias"
) }}
SELECT * FROM com_combinacoes_vazias