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

{{  revelar_combinacoes_implicitas(
    relacao="ate_ultima_competencia",
    agrupar_por=[
        "unidade_geografica_id", 
        "unidade_geografica_id_sus"
    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["estabelecimento_id_scnes"],
        ["profissional_vinculo_ocupacao_id_cbo2002"]
    ],
    cte_resultado="com_combinacoes_vazias"
) }},


-- Passo necessário para remover cbos que não apareceram nenhuma vez em nenhuma competência no município
cbos_zerados_por_municipio AS (
    SELECT
	    unidade_geografica_id_sus,
	    profissional_vinculo_ocupacao_id_cbo2002
    FROM com_combinacoes_vazias
    WHERE quantidade_registrada IS NOT NULL AND quantidade_registrada_anterior IS NOT NULL
    GROUP BY 
	    unidade_geografica_id_sus,
	    profissional_vinculo_ocupacao_id_cbo2002
),
com_combinacoes_vazias_sem_cbos_zerados AS (
    SELECT comb_vazia.*
    FROM com_combinacoes_vazias comb_vazia
    INNER JOIN cbos_zerados_por_municipio cbos_zerados
    ON cbos_zerados.profissional_vinculo_ocupacao_id_cbo2002 = comb_vazia.profissional_vinculo_ocupacao_id_cbo2002 
    AND cbos_zerados.unidade_geografica_id_sus = comb_vazia.unidade_geografica_id_sus
),

final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
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
        coalesce(com_combinacoes_vazias_sem_cbos_zerados.quantidade_registrada, 0) AS quantidade_registrada,
        coalesce(com_combinacoes_vazias_sem_cbos_zerados.quantidade_registrada_anterior, 0) AS quantidade_registrada_anterior,
        (
            coalesce(com_combinacoes_vazias_sem_cbos_zerados.quantidade_registrada, 0)
            - coalesce(com_combinacoes_vazias_sem_cbos_zerados.quantidade_registrada_anterior, 0)
        ) AS dif_quantidade_registrada_anterior,
        now() AS atualizacao_data
    FROM com_combinacoes_vazias_sem_cbos_zerados
)
SELECT * FROM final