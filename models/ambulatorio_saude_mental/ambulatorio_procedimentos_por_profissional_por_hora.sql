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
referencias_atendimentos AS (
    SELECT * FROM {{ ref("ambulatorio_atendimentos") }}
),
atendimentos_por_profissional AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
        realizacao_periodo_data_inicio as periodo_data_inicio,
		estabelecimento_id_cnes,
		profissional_ocupacao_id_cbo AS ocupacao_id_cbo,
        profissional_cns AS profissional_id_cns,  -- TODO: atualizar na fonte
		sum(quantidade_apresentada) AS procedimentos_realizados,
        max(atualizacao_data) AS atualizacao_data
	FROM referencias_atendimentos
    WHERE profissional_cns IS NOT NULL
	GROUP BY 
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
        realizacao_periodo_data_inicio,
		estabelecimento_id_cnes,
		profissional_ocupacao_id_cbo,
        profissional_cns
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
    AND procedimento.estabelecimento_id_cnes
        = disponibilidade.estabelecimento_id_cnes
    AND procedimento.ocupacao_id_cbo = disponibilidade.ocupacao_id_cbo
    AND procedimento.profissional_id_cns = ANY(
        disponibilidade.profissional_ids_cns
    )
),
{{ ultimas_competencias(
    relacao="procedimentos_x_disponibilidade",
    fontes=["bpa_i_disseminacao", "vinculos_profissionais"],
    meses_antes_ultima_competencia=(0, 0),
    cte_resultado="procedimentos_x_disponibilidade_ultimo_mes"
) }},
{# {{ calcular_subtotais(
    relacao="procedimentos_x_disponibilidade_ultimo_mes",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio",
        "profissional_id_cpf_criptografado",
        "profissional_nome"
    ],
    colunas_a_totalizar=[
        "estabelecimento_id_cnes",
        "ocupacao_id_cbo",
    ],
    nomes_categorias_com_totais=[
        "0000000",
        "000000",
    ],
    agregacoes_valores={
        "procedimentos_realizados": "sum",
        "disponibilidade_mensal": "sum",
        "atualizacao_data": "max"
    },
    manter_original=true,
    cte_resultado="procedimentos_x_disponibilidade_com_totais"
) }}, #}
com_procedimentos_por_hora AS (
    SELECT
        *,
		round(
			procedimentos_realizados::numeric
			/ nullif(disponibilidade_mensal, 0)::numeric,
			2
		) AS procedimentos_por_hora
    {# FROM procedimentos_x_disponibilidade_com_totais #}
    FROM procedimentos_x_disponibilidade_ultimo_mes
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id",
            "estabelecimento_id_cnes",
            "ocupacao_id_cbo",
            "profissional_id_cpf_criptografado",
            "profissional_nome"
        ]) }} AS id,
        pph.unidade_geografica_id,
        pph.unidade_geografica_id_sus,
        pph.periodo_id,
        listas_de_codigos.nome_mes(periodo_data_inicio) AS nome_mes,
        pph.periodo_data_inicio AS competencia,
        coalesce(
            estabelecimento.nome_curto,
            estabelecimento.nome,
            'Todos'
        ) AS estabelecimento,
        coalesce(ocupacao.descricao_cbo2002, 'Todas') AS ocupacao,
        profissional_nome,
        pph.procedimentos_realizados,
        pph.procedimentos_por_hora,
        now() AS atualizacao_data
    FROM com_procedimentos_por_hora pph
    LEFT JOIN ocupacoes ocupacao
    ON pph.ocupacao_id_cbo = ocupacao.id_cbo2002
    LEFT JOIN estabelecimentos estabelecimento
    ON pph.estabelecimento_id_cnes = estabelecimento.id_scnes
)
SELECT * FROM final
