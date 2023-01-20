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
)









{{ juntar_periodos_consecutivos(
    relacao="_reducao_danos_acoes_subtotais",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "estabelecimento_id_scnes",
        "profissional_vinculo_ocupacao_id_cbo2002"
    ],
    colunas_valores=[
        "quantidade_registrada",
    ],
    periodo_tipo="Mensal",
    coluna_periodo="periodo_id",    
    colunas_adicionais_periodo=[
        "periodo_data_inicio"
        ],
    cte_resultado="com_periodo_anterior"
) }},
{{ ultimas_competencias(
    relacao="com_periodo_anterior",
    fontes=["procedimentos_disseminacao"],
    meses_antes_ultima_competencia=(0, none),
    cte_resultado="ate_ultima_competencia"
) }},