{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_procedimentos_por_usuario_por_estabelecimento') }}

WITH
{{ preparar_uso_externo(
	relacao="_caps_procedimentos_por_usuario_por_estabelecimento",
	cte_resultado="final"
) }}
SELECT * FROM final