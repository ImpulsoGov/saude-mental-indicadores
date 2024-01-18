
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
sisab_por_tipo_equipe_tipo_producao AS (
	SELECT * FROM {{ source("sisab", "sisab_producao_municipios_por_tipo_equipe_por_tipo_producao") }}
),

{{ selecionar_municipios_ativos(
	relacao="sisab_por_tipo_equipe_tipo_producao",
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
