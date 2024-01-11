{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH 
disponibilidade_por_profissional AS (
    SELECT * FROM {{ ref("disponibilidade_por_profissional") }}
),
estabelecimentos AS (
    SELECT * FROM {{ source("codigos", "estabelecimentos") }}
),
ocupacoes AS (
    SELECT * FROM {{ source("codigos", "ocupacoes") }}
),
ambulatorio_atendimentos AS (
    SELECT * FROM {{ ref("ambulatorio_atendimentos") }}
),
atendimentos_por_profissional AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
        realizacao_periodo_data_inicio as periodo_data_inicio,
		estabelecimento_id_scnes,
		profissional_vinculo_ocupacao_id_cbo2002 AS ocupacao_id_cbo2002,
        profissional_id_cns,
		sum(quantidade_apresentada) AS procedimentos_realizados,
        max(atualizacao_data) AS atualizacao_data
	FROM ambulatorio_atendimentos
    WHERE profissional_id_cns IS NOT NULL
	GROUP BY 
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
        realizacao_periodo_data_inicio,
		estabelecimento_id_scnes,
		profissional_vinculo_ocupacao_id_cbo2002,
        profissional_id_cns
),
procedimentos_x_disponibilidade AS (
    SELECT
        procedimento.*,
        disponibilidade.profissional_id_cpf_criptografado,
        disponibilidade.profissional_nome,
        disponibilidade.disponibilidade_mensal
    FROM atendimentos_por_profissional procedimento
    LEFT JOIN disponibilidade_por_profissional disponibilidade
    ON
        procedimento.unidade_geografica_id
        = disponibilidade.unidade_geografica_id
    AND procedimento.periodo_id = disponibilidade.periodo_id
    AND procedimento.estabelecimento_id_scnes
        = disponibilidade.estabelecimento_id_scnes
    AND procedimento.ocupacao_id_cbo2002 = disponibilidade.ocupacao_id_cbo2002
    AND procedimento.profissional_id_cns = ANY(
        disponibilidade.profissional_ids_cns
    )
),

com_procedimentos_por_hora AS (
    SELECT
        *,
		round(
			procedimentos_realizados::numeric
			/ nullif(disponibilidade_mensal, 0)::numeric,
			2
		) AS procedimentos_por_hora
    FROM procedimentos_x_disponibilidade
    {# FROM procedimentos_x_disponibilidade_ultimo_mes #}
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id",
            "estabelecimento_id_scnes",
            "ocupacao_id_cbo2002",
            "profissional_id_cpf_criptografado",
            "profissional_id_cns",
            "profissional_nome"
        ]) }} AS id,
        *
    FROM com_procedimentos_por_hora pph
)
SELECT * FROM final

