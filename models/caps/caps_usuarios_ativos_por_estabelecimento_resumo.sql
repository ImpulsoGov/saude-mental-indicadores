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
final AS (
    SELECT
		{{ dbt_utils.surrogate_key([
			"usuarios_ativos_por_estabelecimento.periodo_id",
			"usuarios_ativos_por_estabelecimento.estabelecimento_id_scnes"
		]) }} AS id,
        usuarios_ativos_por_estabelecimento.*,
        sexo_predominante.sexo_predominante_id_sigtap,
        sexo_predominante.sexo_predominante_quantidade,
        idade_media.usuarios_idade_media
    FROM usuarios_ativos_por_estabelecimento
    FULL JOIN sexo_predominante
    ON 
        usuarios_ativos_por_estabelecimento.unidade_geografica_id
        = sexo_predominante.unidade_geografica_id
    AND usuarios_ativos_por_estabelecimento.unidade_geografica_id_sus
        = sexo_predominante.unidade_geografica_id_sus
    AND usuarios_ativos_por_estabelecimento.periodo_id
		= sexo_predominante.periodo_id
    AND usuarios_ativos_por_estabelecimento.periodo_data_inicio
        = sexo_predominante.periodo_data_inicio
    AND usuarios_ativos_por_estabelecimento.estabelecimento_id_scnes
        = sexo_predominante.estabelecimento_id_scnes
    FULL JOIN idade_media
    ON 
        sexo_predominante.unidade_geografica_id
		= idade_media.unidade_geografica_id
    AND sexo_predominante.unidade_geografica_id_sus
        = idade_media.unidade_geografica_id_sus
    AND sexo_predominante.periodo_id = idade_media.periodo_id
    AND sexo_predominante.periodo_data_inicio = idade_media.periodo_data_inicio
    AND sexo_predominante.estabelecimento_id_scnes
		= idade_media.estabelecimento_id_scnes
)
SELECT * FROM final
