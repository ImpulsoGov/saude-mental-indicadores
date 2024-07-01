{#
SPDX-FileCopyrightText: 2023 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_adesao_evasao_mensal') }}
-- depends_on: {{ ref('configuracoes_estabelecimentos_ausentes_por_periodos') }}
{%- set tags = ['caps_uso_externo', 'adesao_mensal'] %}

WITH
{{ preparar_uso_externo(
	relacao="_caps_adesao_evasao_mensal",
	cte_resultado="final",
	tags=tags 
) }}
SELECT * FROM final
