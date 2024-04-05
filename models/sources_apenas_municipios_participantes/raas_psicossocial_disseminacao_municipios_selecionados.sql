
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
raas_psicossocial_disseminacao AS (
	SELECT * FROM {{ source("siasus", "raas_psicossocial_disseminacao") }}
),

{{ selecionar_municipios_ativos(
	relacao="raas_psicossocial_disseminacao",
	cte_resultado="municipios_selecionados"
) }},

{{ remover_estabelecimentos_indesejados(
	relacao="municipios_selecionados",
	cte_resultado="municipios_selecionados_filtrados"
) }},

{{ limitar_quantidade_meses(
	relacao="municipios_selecionados_filtrados",
	cte_resultado="final"
) }}

SELECT * FROM final