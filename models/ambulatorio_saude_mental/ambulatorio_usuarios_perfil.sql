{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_ambulatorio_usuarios_perfil') }}

WITH
{{ preparar_uso_externo(
	relacao="_ambulatorio_usuarios_perfil",
	cte_resultado="final"
) }}
SELECT * FROM final