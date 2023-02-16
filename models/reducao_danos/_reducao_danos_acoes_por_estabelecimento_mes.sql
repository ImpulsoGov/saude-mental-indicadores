{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
procedimentos_disseminacao AS (
    SELECT * FROM {{ source('siasus', 'procedimentos_disseminacao') }}
),
_reducao_danos_acoes_por_estabelecimento_mes AS (
    SELECT
    	unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        estabelecimento_id_scnes,
        profissional_vinculo_ocupacao_id_cbo2002,
        sum(quantidade_apresentada) AS quantidade_registrada
        FROM procedimentos_disseminacao
        WHERE procedimento_id_sigtap = '0301080313'
        AND	estabelecimento_tipo_id_sigtap = '70'
        GROUP BY
            unidade_geografica_id,
            unidade_geografica_id_sus,
            periodo_id,
            realizacao_periodo_data_inicio,
            estabelecimento_id_scnes,
            profissional_vinculo_ocupacao_id_cbo2002
),
{{ calcular_subtotais(
    relacao="_reducao_danos_acoes_por_estabelecimento_mes",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio"
    ],
    colunas_a_totalizar=[
		"profissional_vinculo_ocupacao_id_cbo2002",
        "estabelecimento_id_scnes"
	],
    nomes_categorias_com_totais=["000000", "0000000"],
    agregacoes_valores={
        "quantidade_registrada": "sum"
    },
    cte_resultado="_reducao_danos_acoes_subtotais"
) }},
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
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "estabelecimento_id_scnes",
            "periodo_id",
            "profissional_vinculo_ocupacao_id_cbo2002"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        periodo_id,
        periodo_data_inicio,
        profissional_vinculo_ocupacao_id_cbo2002,
        quantidade_registrada,
        quantidade_registrada_anterior,
        (
            coalesce(ate_ultima_competencia.quantidade_registrada, 0)
            - coalesce(ate_ultima_competencia.quantidade_registrada_anterior, 0)
        ) AS dif_quantidade_registrada_anterior,
        now() AS atualizacao_data
    FROM ate_ultima_competencia
)
SELECT * FROM final