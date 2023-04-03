{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_ambulatorio_procedimentos_por_profissional_por_hora') }}

WITH
{{ preparar_uso_externo(
	relacao="_ambulatorio_procedimentos_por_profissional_por_hora",
	cte_resultado="final"
) }}
SELECT * FROM final