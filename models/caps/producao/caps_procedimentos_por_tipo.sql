{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_procedimentos_por_tipo') }}
-- depends_on: {{ ref('configuracoes_estabelecimentos_ausentes_por_periodos') }}
{%- set tags = ['caps_uso_externo', 'procedimentos_por_tipo'] %}

WITH
{{ preparar_uso_externo(
	relacao="_caps_procedimentos_por_tipo",
	cte_resultado="final",
	tags=tags 
) }}
SELECT * FROM final