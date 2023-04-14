{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_atendimendos_individuais AS (
    SELECT * FROM {{ ref("_caps_usuarios_atendimentos_individuais") }}
),
_por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
    	unidade_geografica_id_sus,
    	periodo_id,
    	competencia AS periodo_data_inicio,
    	estabelecimento_id_scnes,
    	count(DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE fez_procedimentos AND NOT procedimentos_alem_individual
        ) AS usuarios_apenas_atendimento_individual,
        count(DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE fez_procedimentos
        ) AS fizeram_algum_procedimento
    FROM usuarios_atendimendos_individuais
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        estabelecimento_id_scnes
),
por_estabelecimento AS (
    SELECT
        *,
        round(
            100 * usuarios_apenas_atendimento_individual::numeric
            / nullif(fizeram_algum_procedimento, 0),
            1
        ) AS perc_apenas_atendimentos_individuais
    FROM _por_estabelecimento
),
todos_estabelecimentos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        '0000000' AS estabelecimento_id_scnes,
        sum(
            usuarios_apenas_atendimento_individual
        ) AS usuarios_apenas_atendimento_individual,
        sum(fizeram_algum_procedimento) AS fizeram_algum_procedimento,
        round(
            100 * sum(usuarios_apenas_atendimento_individual)::numeric
            / nullif(sum(fizeram_algum_procedimento), 0),
            1
        ) AS perc_apenas_atendimentos_individuais
    FROM por_estabelecimento
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio
),
por_estabelecimento_com_total AS (
    SELECT *
    FROM por_estabelecimento
    UNION
    SELECT *
    FROM todos_estabelecimentos
),
{{ juntar_periodos_consecutivos(
    relacao="por_estabelecimento_com_total",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "estabelecimento_id_scnes",
    ],
    colunas_valores=[
        "perc_apenas_atendimentos_individuais"
    ],
    periodo_tipo="Mensal",
    coluna_periodo="periodo_id",    
    colunas_adicionais_periodo=[
        "periodo_data_inicio"
        ],
    cte_resultado="com_total_com_periodo_anterior"
) }},
estabelecimento_maior_taxa AS (
    SELECT 
        DISTINCT ON (
            unidade_geografica_id,
            periodo_id
        )
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_scnes AS estabelecimento_maior_taxa,
        perc_apenas_atendimentos_individuais AS maior_taxa
    FROM por_estabelecimento
    ORDER BY 
        unidade_geografica_id,
        periodo_id,
        perc_apenas_atendimentos_individuais DESC
),
todos_atendimentos_por_caps AS (
    SELECT *
    FROM com_total_com_periodo_anterior
    LEFT JOIN estabelecimento_maior_taxa
    USING (
        unidade_geografica_id,
        periodo_id
    )
),
{{ ultimas_competencias(
    relacao="todos_atendimentos_por_caps",
    fontes=["raas_psicossocial_disseminacao", "bpa_i_disseminacao"],
    meses_antes_ultima_competencia=(0, none),
    cte_resultado="ate_ultima_competencia"
) }},
{{ classificar_caps_linha(
    relacao="ate_ultima_competencia",
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id=none,
    todas_linhas_valor=none,
    cte_resultado="com_linhas_cuidado"
) }},
{{ calcular_subtotais(
    relacao="com_linhas_cuidado",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "estabelecimento_id_scnes",
        "periodo_data_inicio",
        "periodo_id",
        "estabelecimento_maior_taxa"
    ],
    colunas_a_totalizar=[
        "estabelecimento_linha_perfil",
        "estabelecimento_linha_idade"
    ],
    nomes_categorias_com_totais=[
        "Todos", 
        "Todos"
    ],
    agregacoes_valores={
        "perc_apenas_atendimentos_individuais": "max",
        "perc_apenas_atendimentos_individuais_anterior": "max",
        "maior_taxa": "max"
    },
    cte_resultado="com_totais"
) }},
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id",
            "estabelecimento_id_scnes",
            "estabelecimento_linha_perfil",
            "estabelecimento_linha_idade"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,        
        estabelecimento_id_scnes,
        estabelecimento_linha_perfil,
        estabelecimento_linha_idade,
        coalesce(
            perc_apenas_atendimentos_individuais,
            0::bigint
            ) AS perc_apenas_atendimentos_individuais,
        coalesce(
            perc_apenas_atendimentos_individuais_anterior,
            0::bigint
            ) AS perc_apenas_atendimentos_individuais_anterior,
        (
    	coalesce(perc_apenas_atendimentos_individuais, 0)
    	- coalesce(perc_apenas_atendimentos_individuais, 0)
        ) AS dif_perc_apenas_atendimentos_individuais,
        estabelecimento_maior_taxa AS maior_taxa_estabelecimento_id_scnes,
        coalesce(
            maior_taxa,
            0::bigint
            ) AS maior_taxa    
    FROM com_totais
)
SELECT * FROM final