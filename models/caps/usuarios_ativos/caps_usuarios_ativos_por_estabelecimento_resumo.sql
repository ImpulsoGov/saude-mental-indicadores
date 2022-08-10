{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_usuarios_ativos_por_estabelecimento_resumo') }}

WITH
{{ preparar_uso_externo(
	relacao="_caps_usuarios_ativos_por_estabelecimento_resumo",
	cte_resultado="final"
) }}
SELECT * FROM final
