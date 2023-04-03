{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimentos_hora_ocupacao_estabelecimento AS (
    SELECT * FROM {{ ref("_caps_procedimentos_por_hora_por_ocupacao_por_estabelecimento") }}
),

{{ classificar_caps_linha(
    relacao="procedimentos_hora_ocupacao_estabelecimento",
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id=none,
    todas_linhas_valor=none,
    cte_resultado="procedimentos_hora_ocupacao_estabelecimento_linha"
) }},
{{ calcular_subtotais(
    relacao="procedimentos_hora_ocupacao_estabelecimento_linha",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_data_inicio",
        "periodo_id"        
    ],
    colunas_a_totalizar=[
        "estabelecimento_linha_perfil",
        "estabelecimento_linha_idade",
        "estabelecimento_id_scnes",
        "ocupacao_id_cbo2002"
    ],
    nomes_categorias_com_totais=[
        "Todos", 
        "Todos",
        "0000000",
        "000000"
    ],
    agregacoes_valores={
        "procedimentos_registrados_raas": "sum",
        "procedimentos_registrados_bpa": "sum",
        "procedimentos_registrados_total": "sum",
        "horas_disponibilidade_profissionais": "sum"
    },
    cte_resultado="producao_todos_estabelecimentos_todas_ocupacoes"
) }},
{{ juntar_periodos_consecutivos(
    relacao="producao_todos_estabelecimentos_todas_ocupacoes",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "estabelecimento_id_scnes",
        "estabelecimento_linha_perfil",
        "estabelecimento_linha_idade",
        "ocupacao_id_cbo2002"
    ],
    colunas_valores=[
        "procedimentos_registrados_raas",
        "procedimentos_registrados_bpa",
        "procedimentos_registrados_total",
        "horas_disponibilidade_profissionais"
    ],
    periodo_tipo="Mensal",
    coluna_periodo="periodo_id",    
    colunas_adicionais_periodo=[
        "periodo_data_inicio"
        ],
    cte_resultado="producao_com_periodo_anterior"
) }},
{{ ultimas_competencias(
    relacao="producao_com_periodo_anterior",
    fontes=["procedimentos_disseminacao"],
    meses_antes_ultima_competencia=(0, none),
    cte_resultado="producao_ate_ultima_competencia"
) }},
producao_hora_ate_ultima_competencia AS (
    SELECT 
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "estabelecimento_id_scnes",
            "periodo_id"
        ]) }} AS id,
        *,
        round(
                procedimentos_registrados_total::numeric 
                / nullif(horas_disponibilidade_profissionais, 0),
                2
            ) AS procedimentos_por_hora,
        round(
                procedimentos_registrados_total_anterior::numeric 
                / nullif(
                    horas_disponibilidade_profissionais_anterior,
                    0
                ),
                2
            ) AS procedimentos_por_hora_anterior
    FROM producao_ate_ultima_competencia
),
final AS (
    SELECT 
        *,
        (
            procedimentos_registrados_raas
            - procedimentos_registrados_raas_anterior
        ) AS dif_procedimentos_registrados_raas_anterior,
        (
            procedimentos_registrados_bpa
            - procedimentos_registrados_bpa_anterior
        ) AS dif_procedimentos_registrados_bpa_anterior,
        (
            procedimentos_registrados_total
            - procedimentos_registrados_total_anterior
        ) AS dif_procedimentos_registrados_total_anterior,
        round(
            100 * procedimentos_por_hora::numeric
            / nullif(procedimentos_por_hora_anterior, 0),
            1
        ) - 100 AS perc_dif_procedimentos_por_hora_anterior
    FROM producao_hora_ate_ultima_competencia
)
SELECT * FROM final
 
    

    