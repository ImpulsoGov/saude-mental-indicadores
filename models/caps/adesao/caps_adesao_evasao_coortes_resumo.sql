{#
SPDX-FileCopyrightText: 2023 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_adesao_evasao_coortes_resumo') }}
-- depends_on: {{ ref('configuracoes_estabelecimentos_ausentes_por_periodos') }}
{%- set tags = ['caps_uso_externo', 'adesao'] %}

WITH
{{ preparar_uso_externo(
	relacao="_caps_adesao_evasao_coortes_resumo",
	cte_resultado="final",
	tags=tags 
) }}
SELECT * FROM final
