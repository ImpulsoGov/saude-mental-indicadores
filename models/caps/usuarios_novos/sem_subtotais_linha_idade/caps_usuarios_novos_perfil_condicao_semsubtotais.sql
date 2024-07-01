{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_usuarios_novos_perfil_condicao_semsubtotais') }}
-- depends_on: {{ ref('configuracoes_estabelecimentos_ausentes_por_periodos') }}
{%- set tags = ['caps_uso_externo', 'usuarios_novos'] %}

WITH
{{ preparar_uso_externo(
	relacao="_caps_usuarios_novos_perfil_condicao_semsubtotais",
	cte_resultado="final",
	tags=tags 
) }}
SELECT * FROM final
