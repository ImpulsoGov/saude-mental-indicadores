{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
sisab_producao_por_conduta AS (
    SELECT * FROM {{ ref("sisab_producao_conduta_problema_condicao_municipios_selecionados") }} 
),
periodos AS (
    SELECT * FROM {{ source('codigos', 'periodos') }}
),
unidades_geograficas AS (
    SELECT * FROM {{ source('codigos', 'unidades_geograficas') }}
),
saude_mental_encaminhamentos_especializada AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        sum(quantidade_registrada) AS quantidade_registrada,
        'Encaminhamento para CAPS' AS conduta
    FROM sisab_producao_por_conduta
    WHERE 
        conduta  = 'Encaminhamento p/ CAPS'
    AND problema_condicao_avaliada 
        = ANY (ARRAY[
            'Saúde mental'::text,
            'Usuário de álcool'::text,
            'Usuário de outras drogas'::TEXT
        ])
    GROUP BY 
        unidade_geografica_id,
        periodo_id
),
saude_mental_todas_condutas AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        sum(quantidade_registrada) AS quantidade_registrada,
        'Todas' AS conduta
    FROM sisab_producao_por_conduta
    WHERE problema_condicao_avaliada = ANY (ARRAY[
        'Saúde mental',
        'Usuário de álcool',
        'Usuário de outras drogas'
    ])
    GROUP BY 
        unidade_geografica_id,
        periodo_id
),
saude_mental_condutas AS (
    SELECT * FROM saude_mental_encaminhamentos_especializada
    UNION
    SELECT * FROM saude_mental_todas_condutas
),
saude_mental_condutas_joins AS (
    SELECT 
        saude_mental_condutas.unidade_geografica_id,
        unidades_geograficas.id_sus AS unidade_geografica_id_sus,
        periodos.data_inicio AS periodo_data_inicio,
        saude_mental_condutas.periodo_id,
        saude_mental_condutas.conduta,
        saude_mental_condutas.quantidade_registrada
    FROM saude_mental_condutas
    LEFT JOIN periodos 
    ON saude_mental_condutas.periodo_id = periodos.id
    LEFT JOIN unidades_geograficas
    ON saude_mental_condutas.unidade_geografica_id = unidades_geograficas.id
),
{{ juntar_periodos_consecutivos(
    relacao="saude_mental_condutas_joins",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "conduta"
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
    fontes=["sisab_producao_conduta_problema_condicao"],
    meses_antes_ultima_competencia=(0, none),
    cte_resultado="ate_ultima_competencia"
) }},
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id",
            "conduta"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_data_inicio,
        periodo_id,
        conduta,
        quantidade_registrada,
        quantidade_registrada_anterior,
        (
            coalesce(quantidade_registrada, 0)
            - coalesce(quantidade_registrada_anterior, 0)
        ) AS dif_quantidade_registrada_anterior,
        now() AS atualizacao_data   
    FROM ate_ultima_competencia
)  
SELECT * FROM final