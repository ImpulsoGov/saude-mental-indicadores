{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_ativos AS (
    SELECT * FROM {{ ref("caps_usuarios_ativos") }}
),
usuarios_ativos_por_estabelecimento AS (
	SELECT * FROM {{ ref("caps_usuarios_ativos_por_estabelecimento") }}
),
{{ classificar_caps_linha(
    relacao="usuarios_ativos",
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id=none,
    todas_linhas_valor=none,
    cte_resultado="usuarios_ativos_com_linhas_cuidado"
) }},
{{ calcular_subtotais(
    relacao="usuarios_ativos_com_linhas_cuidado",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio",
    ],
    colunas_a_totalizar=[
		"estabelecimento_linha_perfil",
    	"estabelecimento_linha_idade",
		"estabelecimento_id_scnes",
	],
    nomes_categorias_com_totais=["Todos", "Todos", "0000000"],
    agregacoes_valores={
        "usuario_idade": "avg",
        "usuario_sexo_id_sigtap": "mode() WITHIN GROUP (ORDER BY",
    },
    manter_original=true,
    cte_resultado="caracteristicas_predominantes"
) }},
{{ classificar_caps_linha(
    relacao="usuarios_ativos_por_estabelecimento",
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id=none,
    todas_linhas_valor=none,
    cte_resultado="usuarios_ativos_por_estabelecimento_com_linhas_cuidado"
) }},
{{ calcular_subtotais(
    relacao="usuarios_ativos_por_estabelecimento_com_linhas_cuidado",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio",
    ],
    colunas_a_totalizar=[
		"estabelecimento_linha_perfil",
    	"estabelecimento_linha_idade",
		"estabelecimento_id_scnes",
	],
    nomes_categorias_com_totais=["Todos", "Todos", "0000000"],
    agregacoes_valores={
        "ativos_mes": "sum",
        "ativos_3meses": "sum",
        "tornandose_inativos": "sum",
        "ativos_mes_anterior": "sum",
        "ativos_3meses_anterior": "sum",
        "tornandose_inativos_anterior": "sum",
        "dif_ativos_mes_anterior": "sum",
        "dif_ativos_3meses_anterior": "sum",
        "dif_tornandose_inativos_anterior": "sum",
    },
    manter_original=true,
    cte_resultado="contagem_usuarios"
) }},
{# resumo #}final AS (
    SELECT
		{{ dbt_utils.surrogate_key([
			"unidade_geografica_id",
			"estabelecimento_linha_perfil",
			"estabelecimento_linha_idade",
			"estabelecimento_id_scnes",
			"periodo_id"
		]) }} AS id,
        unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_linha_perfil,
		estabelecimento_linha_idade,
		estabelecimento_id_scnes,
		ativos_mes,
		ativos_3meses,
		tornandose_inativos,
		ativos_mes_anterior,
		ativos_3meses_anterior,
		tornandose_inativos_anterior,
		dif_ativos_mes_anterior,
		dif_ativos_3meses_anterior,
		dif_tornandose_inativos_anterior,
        usuario_sexo_id_sigtap as sexo_predominante_id_sigtap,
        usuario_idade as usuarios_idade_media,
		now() AS atualizacao_data
    FROM caracteristicas_predominantes
    FULL JOIN contagem_usuarios
    USING (
        unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_linha_perfil,
    	estabelecimento_linha_idade,
		estabelecimento_id_scnes
	)
)
SELECT * FROM final
