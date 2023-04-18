{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimentos_usuario_mes AS (
    SELECT * FROM {{ ref("_caps_procedimentos_por_usuario_por_mes") }}
),
por_estabelecimento AS (
    SELECT
    	unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
    	periodo_id,
    	estabelecimento_id_scnes,
    	sum(
    	   procedimentos_exceto_acolhimento
    	) AS procedimentos_exceto_acolhimento,
    	count(DISTINCT usuario_id_cns_criptografado) AS ativos_mes,
    	round(
    		(
    			sum(procedimentos_exceto_acolhimento)::numeric
    			/ nullif(count(DISTINCT usuario_id_cns_criptografado), 0)
    		),
    		1
    	) AS procedimentos_por_usuario
    FROM procedimentos_usuario_mes
    WHERE (
        procedimentos_registrados_raas 
        + procedimentos_registrados_bpa_i 
        - acolhimentos_iniciais_em_caps
    ) > 0
    GROUP BY
    	unidade_geografica_id,
    	unidade_geografica_id_sus,
    	estabelecimento_id_scnes,
    	competencia,
        periodo_id
),
todos_estabelecimentos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        '0000000' AS estabelecimento_id_scnes,
        sum(
            procedimentos_exceto_acolhimento
        ) AS procedimentos_exceto_acolhimento,
        sum(ativos_mes) AS ativos_mes,
        round(
            (
                sum(procedimentos_exceto_acolhimento)::numeric
                / nullif(sum(ativos_mes), 0)
            ),
            1
        ) AS procedimentos_por_usuario
    FROM por_estabelecimento
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id
),
por_estabelecimento_com_total AS (
    SELECT *
    FROM por_estabelecimento
    UNION
    SELECT *
    FROM todos_estabelecimentos
),
caps_maior_taxa AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id,
            periodo_id
        )
        unidade_geografica_id,
        periodo_id,
        coalesce(
            estabelecimento_id_scnes
        ) AS maior_taxa_estabelecimento_id_scnes,
        procedimentos_por_usuario AS maior_taxa
        FROM por_estabelecimento
        ORDER BY
            unidade_geografica_id,
            periodo_id,
            procedimentos_por_usuario DESC
),
{{ juntar_periodos_consecutivos(
    relacao="por_estabelecimento_com_total",
    agrupar_por=[
        "unidade_geografica_id", 
        "unidade_geografica_id_sus", 
        "estabelecimento_id_scnes"
    ],
    colunas_valores=[
        "procedimentos_exceto_acolhimento",
        "ativos_mes",
        "procedimentos_por_usuario"
    ],
    periodo_tipo="Mensal",
    coluna_periodo="periodo_id",    
    cte_resultado="com_periodo_anterior"
) }},
procedimentos_com_maior_taxa AS (
    SELECT *
    FROM com_periodo_anterior
    LEFT JOIN caps_maior_taxa
    USING (
        unidade_geografica_id,
        periodo_id
    )
),
{{ ultimas_competencias(
    relacao="procedimentos_com_maior_taxa",
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
        "maior_taxa_estabelecimento_id_scnes"      
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
        "procedimentos_exceto_acolhimento": "max",
        "procedimentos_exceto_acolhimento_anterior": "max",
        "ativos_mes": "max",
        "ativos_mes_anterior": "max",
        "procedimentos_por_usuario": "max",
        "procedimentos_por_usuario_anterior": "max",
        "maior_taxa": "max"
    },
    cte_resultado="com_totais"
) }},
final AS (
    SELECT 
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "estabelecimento_id_scnes",
            "periodo_id",
            "estabelecimento_linha_perfil",
            "estabelecimento_linha_idade"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        procedimentos_exceto_acolhimento,
        procedimentos_exceto_acolhimento_anterior,
        ativos_mes,
        ativos_mes_anterior,
        procedimentos_por_usuario,
        procedimentos_por_usuario_anterior,
        maior_taxa,
        maior_taxa_estabelecimento_id_scnes,
        estabelecimento_linha_perfil,
        estabelecimento_linha_idade,
        round(
            100 * (
                coalesce(procedimentos_por_usuario, 0)
                - coalesce(procedimentos_por_usuario_anterior, 0)
            )::numeric
            / nullif(
                coalesce(procedimentos_por_usuario_anterior, 0),
                0
            ), 1
            ) AS dif_procedimentos_por_usuario_anterior_perc,
        now() AS atualizacao_data,
        estabelecimento_id_scnes  
    FROM com_totais
)
SELECT * FROM final