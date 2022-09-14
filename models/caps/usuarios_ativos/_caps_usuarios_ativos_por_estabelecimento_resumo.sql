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
usuarios_ativos_desconsiderar_estabelecimento AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		'0000000' AS estabelecimento_id_scnes,
		usuario_sexo_id_sigtap,
		usuario_idade,
		usuario_id_cns_criptografado,
		ativo_3meses
	FROM usuarios_ativos
),
usuarios_ativos_com_soma_estabelecimentos AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes,
		usuario_sexo_id_sigtap,
		usuario_idade,
		usuario_id_cns_criptografado,
		ativo_3meses
	FROM usuarios_ativos
	UNION
	SELECT *
	FROM usuarios_ativos_desconsiderar_estabelecimento
),
sexo_predominante AS (
	SELECT DISTINCT ON (
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes
	)
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes,
		usuario_sexo_id_sigtap AS sexo_predominante_id_sigtap,
		usuarios_sexo_quantidade AS sexo_predominante_quantidade
	FROM (
		SELECT
			unidade_geografica_id,
			unidade_geografica_id_sus,
			periodo_id,
			periodo_data_inicio,
			estabelecimento_id_scnes,
			usuario_sexo_id_sigtap,
			count(
                DISTINCT usuario_id_cns_criptografado
            ) AS usuarios_sexo_quantidade
		FROM usuarios_ativos_com_soma_estabelecimentos
		WHERE ativo_3meses
		GROUP BY 
			unidade_geografica_id,
			unidade_geografica_id_sus,
			periodo_id,
			periodo_data_inicio,
			estabelecimento_id_scnes,
			usuario_sexo_id_sigtap
		) AS t
	ORDER BY
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes,
		usuarios_sexo_quantidade DESC
),
idade_media AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes,
		round(avg(usuario_idade)) AS usuarios_idade_media
	FROM usuarios_ativos_com_soma_estabelecimentos
	WHERE ativo_3meses
	GROUP BY 
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes
),
resumo AS (
    SELECT
		{{ dbt_utils.surrogate_key([
			"unidade_geografica_id",
			"estabelecimento_id_scnes",
			"periodo_id"
		]) }} AS id,
        unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes,
		ativos_mes,
		ativos_3meses,
		tornandose_inativos,
		ativos_mes_anterior,
		ativos_3meses_anterior,
		tornandose_inativos_anterior,
		(ativos_mes - ativos_mes_anterior) AS dif_ativos_mes_anterior,
		(ativos_3meses - ativos_3meses_anterior) AS dif_ativos_3meses_anterior,
		(
			tornandose_inativos - tornandose_inativos_anterior
		) AS dif_tornandose_inativos_anterior,
        sexo_predominante.sexo_predominante_id_sigtap,
        sexo_predominante.sexo_predominante_quantidade,
        idade_media.usuarios_idade_media,
		now() AS atualizacao_data
    FROM usuarios_ativos_por_estabelecimento
    FULL JOIN sexo_predominante
    USING (
        unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes
	)
    FULL JOIN idade_media
    USING ( 
        unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_scnes
	)
),
{{ ultimas_competencias(
    relacao="resumo",
    fontes=[
		"raas_psicossocial_disseminacao",
		"bpa_i_disseminacao"
	],
    meses_antes_ultima_competencia=(0, none),
    cte_resultado="final"
) }}
SELECT * FROM final
