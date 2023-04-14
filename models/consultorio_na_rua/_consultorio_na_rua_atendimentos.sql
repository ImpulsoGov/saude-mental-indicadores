{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
sisab_producao_municipios_por_tipo_equipe_por_tipo_producao AS (
    SELECT * FROM {{ source('sisab', 'sisab_producao_municipios_por_tipo_equipe_por_tipo_producao') }}
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
{{ ultimas_competencias(
    relacao="com_periodo_anterior",
    fontes=["sisab_producao_municipios_equipe_producao"],
    meses_antes_ultima_competencia=(0, none),
    cte_resultado="ate_ultima_competencia"
) }},
{{ calcular_subtotais(
    relacao="ate_ultima_competencia",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio",
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
    cte_resultado="ate_ultima_competencia_com_substotais"
) }},
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id",
            "tipo_producao"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_data_inicio,
        periodo_id,
        tipo_producao,
        ate_ultima_competencia_com_substotais.quantidade_registrada,
        ate_ultima_competencia_com_substotais.quantidade_registrada_anterior,
        (
            coalesce(ate_ultima_competencia_com_substotais.quantidade_registrada, 0)
            - coalesce(ate_ultima_competencia_com_substotais.quantidade_registrada_anterior, 0)
        ) AS dif_quantidade_registrada_anterior,
        now() AS atualizacao_data   
    FROM ate_ultima_competencia_com_substotais
)  
SELECT * FROM final