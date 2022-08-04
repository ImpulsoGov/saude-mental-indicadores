{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_ativos AS (
    SELECT * FROM {{ ref('caps_usuarios_ativos') }}
),
por_estabelecimentos AS (
	SELECT 
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes,
		count (DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE ativo_mes
        ) AS ativos_mes,
		count (DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE ativo_3meses
        ) AS ativos_3meses,
		count (DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE tornandose_inativo
        ) AS tornandose_inativos,
        max(atualizacao_data) AS atualizacao_data
	FROM usuarios_ativos
	GROUP BY 
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes
),
todos_estabelecimentos_por_municipio AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        '0000000' AS estabelecimento_id_scnes,
        sum(ativos_mes) AS ativos_mes,
        sum(ativos_3meses) AS ativos_3meses,
        sum(tornandose_inativos) AS tornandose_inativos,
        max(atualizacao_data) AS atualizacao_data
    FROM por_estabelecimentos
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio
),
por_estabelecimento_com_total_municipio AS (
    SELECT *
    FROM por_estabelecimentos
    UNION
    SELECT *
    FROM todos_estabelecimentos_por_municipio
),
{{ juntar_periodos_consecutivos(
	relacao="por_estabelecimento_com_total_municipio",
	agrupar_por=[
		"unidade_geografica_id",
		"unidade_geografica_id_sus",
		"estabelecimento_id_scnes"
	],
	colunas_valores=[
		"ativos_3meses",
	],
	periodo_tipo="Mensal",
	coluna_periodo="periodo_id",
	colunas_adicionais_periodo=["periodo_data_inicio"],
	cte_resultado="comparacao_periodo_anterior"
) }},
{{ ultimas_competencias(
    relacao="comparacao_periodo_anterior",
    fontes=[
		"raas_psicossocial_disseminacao",
		"bpa_i_disseminacao"
	],
    meses_antes_ultima_competencia=(0, none),
    cte_resultado="final"
) }}
SELECT * FROM final
