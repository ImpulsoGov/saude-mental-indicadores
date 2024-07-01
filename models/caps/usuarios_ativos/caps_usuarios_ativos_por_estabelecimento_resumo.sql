{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_usuarios_ativos_por_estabelecimento_resumo') }}
-- depends_on: {{ ref('configuracoes_estabelecimentos_ausentes_por_periodos') }}
{%- set tags = ['caps_uso_externo', 'usuarios_ativos_resumo'] %}

WITH
{{ preparar_uso_externo(
	relacao="_caps_usuarios_ativos_por_estabelecimento_resumo",
	cte_resultado="final",
	tags=tags 
) }}
SELECT * FROM final
