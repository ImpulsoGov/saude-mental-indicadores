{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
periodos_sucessao AS (
    SELECT * FROM {{ ref('periodos_sucessao') }}
),
vinculos_profissionais AS (
    SELECT *
    FROM {{ ref('vinculos_profissionais_municipios_selecionados') }}
),
disponibilidade_por_dia_util AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_scnes,
        ocupacao_id_cbo2002,
		profissional_id_cpf_criptografado,
		array_agg(profissional_id_cns) as profissional_ids_cns,
        profissional_nome,
        sum(
            atendimento_carga_ambulatorial
            + atendimento_carga_outras
        ) / 5 AS horas_disponibilidade_diaria
    FROM vinculos_profissionais
    WHERE atendimento_sus
    GROUP BY 
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_scnes,
        ocupacao_id_cbo2002,
		profissional_id_cpf_criptografado,
        profissional_nome
),
final AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_scnes,
        ocupacao_id_cbo2002,
		profissional_id_cpf_criptografado,
		profissional_ids_cns,
        profissional_nome,
        (
            horas_disponibilidade_diaria
            * {{ schema }}.diferenca_dias_uteis(
                periodos_sucessao.periodo_data_inicio,
                periodos_sucessao.proximo_periodo_data_inicio
            )
        ) AS disponibilidade_mensal
    FROM disponibilidade_por_dia_util
    LEFT JOIN periodos_sucessao
    USING ( periodo_id )
)
SELECT * FROM final
