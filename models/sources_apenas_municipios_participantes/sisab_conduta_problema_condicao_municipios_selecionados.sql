
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
sisab_producao_conduta_problema_condicao AS (
	SELECT
		conduta,
		problema_condicao_avaliada,
		id,
		periodo_id,
		unidade_geografica_id,
		tipo_producao,
		quantidade_registrada,
		atualizacao_data
	FROM {{ source("sisab", "sisab_producao_conduta_problema_condicao") }}
),

{{ selecionar_municipios_ativos(
	relacao="sisab_producao_conduta_problema_condicao",
	coluna_municipio_id="unidade_geografica_id",
	cte_resultado="municipios_selecionados"
) }},

{{ limitar_quantidade_meses(
	relacao="municipios_selecionados",
	coluna_data="periodo_id",
	coluna_municipio_id="unidade_geografica_id",
	cte_resultado="final"
) }}
SELECT * FROM final
