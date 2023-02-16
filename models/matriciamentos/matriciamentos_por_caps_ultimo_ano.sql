{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_matriciamentos_por_caps_ultimo_ano') }}

WITH
{{ preparar_uso_externo(
	relacao="_matriciamentos_por_caps_ultimo_ano",
	cte_resultado="final"
) }}
SELECT * FROM final