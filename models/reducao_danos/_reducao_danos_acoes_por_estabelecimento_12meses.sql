{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
procedimentos_disseminacao AS (
    SELECT * FROM {{ ref("procedimentos_disseminacao_municipios_selecionados") }}
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
{{ juntar_periodos_consecutivos(
    relacao="_reducao_danos_acoes_por_estabelecimento_mes",
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


-- Calcula ações para os últimos 12 meses 
{{ ultimas_competencias(
    relacao="com_periodo_anterior",
    fontes=["procedimentos_disseminacao"],
    meses_antes_ultima_competencia=(0, 11),
    cte_resultado="reducao_danos_12meses"
) }},
{{ calcular_subtotais(
    relacao="reducao_danos_12meses",
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
    cte_resultado="reducao_danos_12meses_subtotais"
) }},
{{  revelar_combinacoes_implicitas(
    relacao="reducao_danos_12meses_subtotais",
    agrupar_por=[

    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["unidade_geografica_id", "unidade_geografica_id_sus"],
        ["estabelecimento_id_scnes"],
        ["profissional_vinculo_ocupacao_id_cbo2002"]
    ],
    cte_resultado="reducao_danos_12meses_com_combinacoes_vazias"
) }},

-- Calcula ações os últimos 12 a 24 meses
{{ ultimas_competencias(
    relacao="com_periodo_anterior",
    fontes=["procedimentos_disseminacao"],
    meses_antes_ultima_competencia=(12, 23),
    cte_resultado="reducao_danos_12a24meses"
) }},
{{ calcular_subtotais(
    relacao="reducao_danos_12a24meses",
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
    cte_resultado="reducao_danos_12a24meses_subtotais"
) }},
{{  revelar_combinacoes_implicitas(
    relacao="reducao_danos_12a24meses_subtotais",
    agrupar_por=[

    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["unidade_geografica_id", "unidade_geografica_id_sus"],
        ["estabelecimento_id_scnes"],
        ["profissional_vinculo_ocupacao_id_cbo2002"]
    ],
    cte_resultado="reducao_danos_12a24meses_com_combinacoes_vazias"
) }},


reducao_danos_12meses_agrupado AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        profissional_vinculo_ocupacao_id_cbo2002,
        estabelecimento_id_scnes,
        min(periodo_data_inicio) AS a_partir_de,
        max(periodo_data_inicio) AS ate,
        sum(quantidade_registrada) AS quantidade_registrada
    FROM reducao_danos_12meses_com_combinacoes_vazias
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        profissional_vinculo_ocupacao_id_cbo2002,
        estabelecimento_id_scnes
),

reducao_danos_12a24meses_agrupado AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        profissional_vinculo_ocupacao_id_cbo2002,
        estabelecimento_id_scnes,
        min(periodo_data_inicio) AS a_partir_de,
        max(periodo_data_inicio) AS ate,
        sum(quantidade_registrada) AS quantidade_registrada_anterior
    FROM reducao_danos_12a24meses_com_combinacoes_vazias
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        profissional_vinculo_ocupacao_id_cbo2002,
        estabelecimento_id_scnes
),
reducao_danos_12meses_agrupado_comperiodoanterior AS (
    SELECT
        reducao_danos_12meses_agrupado.unidade_geografica_id,
        reducao_danos_12meses_agrupado.unidade_geografica_id_sus,
        reducao_danos_12meses_agrupado.profissional_vinculo_ocupacao_id_cbo2002,
        reducao_danos_12meses_agrupado.estabelecimento_id_scnes,
        reducao_danos_12meses_agrupado.a_partir_de,
        reducao_danos_12meses_agrupado.ate,
        EXTRACT(
            YEAR FROM reducao_danos_12meses_agrupado.a_partir_de
        )::text AS a_partir_do_ano,
        EXTRACT(
            YEAR FROM reducao_danos_12meses_agrupado.ate
        )::text AS ate_ano,
        listas_de_codigos.nome_mes(reducao_danos_12meses_agrupado.a_partir_de::date) AS a_partir_do_mes,
        listas_de_codigos.nome_mes(reducao_danos_12meses_agrupado.ate::date) AS ate_mes,
        coalesce(reducao_danos_12meses_agrupado.quantidade_registrada, 0) AS quantidade_registrada,
        coalesce(reducao_danos_12a24meses_agrupado.quantidade_registrada_anterior, 0) AS quantidade_registrada_anterior,
        (
            coalesce(reducao_danos_12meses_agrupado.quantidade_registrada, 0)
            - coalesce(reducao_danos_12a24meses_agrupado.quantidade_registrada_anterior, 0)
        ) AS dif_quantidade_registrada_anterior,
        now() AS atualizacao_data
    FROM reducao_danos_12meses_agrupado
    LEFT JOIN reducao_danos_12a24meses_agrupado
    ON
        reducao_danos_12meses_agrupado.unidade_geografica_id_sus = reducao_danos_12a24meses_agrupado.unidade_geografica_id_sus AND
        reducao_danos_12meses_agrupado.profissional_vinculo_ocupacao_id_cbo2002 = reducao_danos_12a24meses_agrupado.profissional_vinculo_ocupacao_id_cbo2002 AND
        reducao_danos_12meses_agrupado.estabelecimento_id_scnes = reducao_danos_12a24meses_agrupado.estabelecimento_id_scnes
),

final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
				"unidade_geografica_id",
                "profissional_vinculo_ocupacao_id_cbo2002",
                "estabelecimento_id_scnes",
                "a_partir_de",
                "ate"
		]) }} AS id,
		*
    FROM reducao_danos_12meses_agrupado_comperiodoanterior
)
SELECT * FROM final