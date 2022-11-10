{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_ativos AS (
    SELECT * FROM {{ ref('caps_usuarios_ativos') }}
	WHERE usuario_primeiro_procedimento_periodo_data_inicio IS NOT NULL
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
{{ juntar_periodos_consecutivos(
	relacao="por_estabelecimentos",
	agrupar_por=[
		"unidade_geografica_id",
		"unidade_geografica_id_sus",
		"estabelecimento_id_scnes"
	],
	colunas_valores=[
        "ativos_mes",
		"ativos_3meses",
        "tornandose_inativos"
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
    cte_resultado="ate_ultima_competencia"
) }},
final AS (
    SELECT
        *,
        (ativos_mes - ativos_mes_anterior) AS dif_ativos_mes_anterior,
        (ativos_3meses - ativos_3meses_anterior) AS dif_ativos_3meses_anterior,
        (
            tornandose_inativos - tornandose_inativos_anterior
        ) AS dif_tornandose_inativos_anterior
    FROM ate_ultima_competencia
)
SELECT * FROM final
