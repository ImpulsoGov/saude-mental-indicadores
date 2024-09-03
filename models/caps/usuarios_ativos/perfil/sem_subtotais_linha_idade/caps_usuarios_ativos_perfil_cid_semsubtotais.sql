{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_usuarios_ativos_perfil_cid_semsubtotais') }}
-- depends_on: {{ ref('configuracoes_estabelecimentos_ausentes_por_periodos') }}
{%- set tags = ['caps_uso_externo', 'usuarios_ativos'] %}

WITH
{{ preparar_uso_externo(
	relacao="_caps_usuarios_ativos_perfil_cid_semsubtotais",
	cte_resultado="final",
	tags=tags 
) }}
SELECT * FROM final
