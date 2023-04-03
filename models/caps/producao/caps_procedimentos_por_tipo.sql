{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_procedimentos_por_tipo') }}

WITH
{{ preparar_uso_externo(
	relacao="_caps_procedimentos_por_tipo",
	cte_resultado="final"
) }}
SELECT * FROM final