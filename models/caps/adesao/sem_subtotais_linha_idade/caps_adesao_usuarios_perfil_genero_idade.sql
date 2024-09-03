{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_adesao_usuarios_perfil_genero_idade') }}
-- depends_on: {{ ref('configuracoes_estabelecimentos_ausentes_por_periodos') }}
{%- set tags = ['caps_uso_externo', 'adesao_mensal'] %}

WITH
{{ preparar_uso_externo(
	relacao="_caps_adesao_usuarios_perfil_genero_idade",
	cte_resultado="final",
	tags=tags 
) }}
SELECT * FROM final
