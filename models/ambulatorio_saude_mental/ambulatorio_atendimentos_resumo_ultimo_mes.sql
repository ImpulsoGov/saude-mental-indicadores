{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
ambulatorio_atendimentos_resumo AS (
SELECT
{{ renomear_colunas(
    relacao=ref("ambulatorio_atendimentos_resumo"),
    colunas_a_renomear={"competencia": "periodo_data_inicio"}
) }}
FROM {{ ref("ambulatorio_atendimentos_resumo") }}
),
{{ ultimas_competencias(
    relacao="ambulatorio_atendimentos_resumo",
    fontes=["bpa_i_disseminacao", "vinculos_profissionais"],
    meses_antes_ultima_competencia=(0, 0),
    cte_resultado="ultima_competencia"
) }},
final AS (
SELECT
{%- for coluna in adapter.get_columns_in_relation(
    ref("ambulatorio_atendimentos_resumo")
) %}
    {{ coluna.name
        if coluna.name != "competencia"
        else "periodo_data_inicio AS competencia"
    }}{{ "," if not loop.last }}
{%- endfor %}
FROM ultima_competencia
)
SELECT * FROM final
