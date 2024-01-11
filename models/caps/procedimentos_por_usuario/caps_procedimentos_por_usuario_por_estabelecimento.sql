{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_procedimentos_por_usuario_por_estabelecimento') }}

WITH
{{ preparar_uso_externo(
	relacao="_caps_procedimentos_por_usuario_por_estabelecimento",
	cte_resultado="intermediaria"
) }},
final AS (
	SELECT 
		id,
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		competencia,
		-- procedimentos_exceto_acolhimento,
		-- procedimentos_exceto_acolhimento_anterior,
		-- ativos_mes,
		-- ativos_mes_anterior,
		procedimentos_por_usuario,
		-- procedimentos_por_usuario_anterior,
		maior_taxa,
		maior_taxa_estabelecimento,
		estabelecimento_linha_perfil,
		estabelecimento_linha_idade,
		dif_procedimentos_por_usuario_anterior_perc,
		atualizacao_data,
		estabelecimento,
		periodo,
		nome_mes,
		periodo_ordem
	FROM intermediaria
)
SELECT * FROM final