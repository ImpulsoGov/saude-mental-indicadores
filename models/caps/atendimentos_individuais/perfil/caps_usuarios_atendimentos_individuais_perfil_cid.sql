{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_usuarios_atendimentos_individuais_perfil_cid') }}

WITH
{{ preparar_uso_externo(
	relacao="_caps_usuarios_atendimentos_individuais_perfil_cid",
	cte_resultado="final"
) }}
SELECT * FROM final
