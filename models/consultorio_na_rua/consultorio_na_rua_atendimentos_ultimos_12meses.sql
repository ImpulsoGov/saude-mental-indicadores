{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
sisab_producao_municipios_por_tipo_equipe_por_tipo_producao AS (
    SELECT * FROM {{ ref("sisab_por_tipo_equipe_tipo_producao_municipios_selecionados") }} 
),
periodos AS (
    SELECT * FROM {{ source('codigos', 'periodos') }}
),
unidades_geograficas AS (
    SELECT * FROM {{ source('codigos', 'unidades_geograficas') }}
),
atendimentos AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        tipo_equipe,
        tipo_producao,
        sum(quantidade_registrada) AS quantidade_registrada
    FROM sisab_producao_municipios_por_tipo_equipe_por_tipo_producao
    WHERE
        tipo_equipe = 'Eq. Consultório na Rua - ECR'
    GROUP BY
        unidade_geografica_id,
        periodo_id,
        tipo_equipe,
        tipo_producao
        ),
atendimentos_com_joins AS (
    SELECT
        atendimentos.unidade_geografica_id,
        unidade_geografica.id_sus AS unidade_geografica_id_sus,
        periodos.data_inicio AS periodo_data_inicio,
        periodos.id AS periodo_id,
        atendimentos.tipo_equipe,
        atendimentos.tipo_producao,
        atendimentos.quantidade_registrada
    FROM atendimentos
    LEFT JOIN periodos
    ON 
        atendimentos.periodo_id = periodos.id
    AND periodos.tipo = 'Mensal'
    LEFT JOIN listas_de_codigos.unidades_geograficas unidade_geografica
    ON atendimentos.unidade_geografica_id = unidade_geografica.id
    ),
{{ juntar_periodos_consecutivos(
    relacao="atendimentos_com_joins",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "tipo_producao"
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
    fontes=["sisab_producao_municipios_equipe_producao"],
    meses_antes_ultima_competencia=(0, 11),
    cte_resultado="cnr_12meses"
) }},
{{ calcular_subtotais(
    relacao="cnr_12meses",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio"
    ],
    colunas_a_totalizar=[
		"tipo_producao"
	],
    nomes_categorias_com_totais=["Todos"],
    agregacoes_valores={
        "quantidade_registrada": "sum",
        "quantidade_registrada_anterior": "sum"
    },
    manter_original=true,
    cte_resultado="cnr_12meses_subtotais"
) }},
{{  revelar_combinacoes_implicitas(
    relacao="cnr_12meses_subtotais",
    agrupar_por=[

    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["unidade_geografica_id", "unidade_geografica_id_sus"],
        ["tipo_producao"]
    ],
    cte_resultado="cnr_12meses_subtotais_com_combinacoes_vazias"
) }},



-- Calcula ações os últimos 12 a 24 meses
{{ ultimas_competencias(
    relacao="com_periodo_anterior",
    fontes=["sisab_producao_municipios_equipe_producao"],
    meses_antes_ultima_competencia=(12, 23),
    cte_resultado="cnr_12a24meses"
) }},
{{ calcular_subtotais(
    relacao="cnr_12a24meses",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio"
    ],
    colunas_a_totalizar=[
		"tipo_producao"
	],
    nomes_categorias_com_totais=["Todos"],
    agregacoes_valores={
        "quantidade_registrada": "sum",
        "quantidade_registrada_anterior": "sum"
    },
    manter_original=true,
    cte_resultado="cnr_12a24meses_subtotais"
) }},
{{  revelar_combinacoes_implicitas(
    relacao="cnr_12a24meses_subtotais",
    agrupar_por=[

    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["unidade_geografica_id", "unidade_geografica_id_sus"],
        ["tipo_producao"]
    ],
    cte_resultado="cnr_12a24meses_subtotais_com_combinacoes_vazias"
) }},


cnr_12meses_agrupado AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        tipo_producao,
        min(periodo_data_inicio) AS a_partir_de,
        max(periodo_data_inicio) AS ate,
        sum(quantidade_registrada) AS quantidade_registrada
    FROM cnr_12meses_subtotais_com_combinacoes_vazias
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        tipo_producao
),
cnr_12a24meses_agrupado AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        tipo_producao,
        min(periodo_data_inicio) AS a_partir_de,
        max(periodo_data_inicio) AS ate,
        sum(quantidade_registrada) AS quantidade_registrada_anterior
    FROM cnr_12a24meses_subtotais_com_combinacoes_vazias
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        tipo_producao
),
cnr_12meses_agrupado_comperiodoanterior AS (
    SELECT
        cnr_12meses_agrupado.unidade_geografica_id,
        cnr_12meses_agrupado.unidade_geografica_id_sus,
        cnr_12meses_agrupado.tipo_producao,
        cnr_12meses_agrupado.a_partir_de,
        cnr_12meses_agrupado.ate,
        EXTRACT(
            YEAR FROM cnr_12meses_agrupado.a_partir_de
        )::text AS a_partir_do_ano,
        EXTRACT(
            YEAR FROM cnr_12meses_agrupado.ate
        )::text AS ate_ano,
        listas_de_codigos.nome_mes(cnr_12meses_agrupado.a_partir_de::date) AS a_partir_do_mes,
        listas_de_codigos.nome_mes(cnr_12meses_agrupado.ate::date) AS ate_mes,
        coalesce(cnr_12meses_agrupado.quantidade_registrada, 0) AS quantidade_registrada,
        coalesce(cnr_12a24meses_agrupado.quantidade_registrada_anterior, 0) AS quantidade_registrada_anterior,
        (
            coalesce(cnr_12meses_agrupado.quantidade_registrada, 0)
            - coalesce(cnr_12a24meses_agrupado.quantidade_registrada_anterior, 0)
        ) AS dif_quantidade_registrada_anterior
    FROM cnr_12meses_agrupado
    LEFT JOIN cnr_12a24meses_agrupado
    ON
        cnr_12meses_agrupado.unidade_geografica_id_sus = cnr_12a24meses_agrupado.unidade_geografica_id_sus AND
        cnr_12meses_agrupado.tipo_producao = cnr_12a24meses_agrupado.tipo_producao
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "a_partir_de",
            "ate",
            "tipo_producao"
        ]) }} AS id,
        *,
        now() AS atualizacao_data
    FROM cnr_12meses_agrupado_comperiodoanterior
)
SELECT * FROM final