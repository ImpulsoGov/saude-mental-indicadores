{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_ambulatorio_atendimentos_resumo') }}

WITH
{{ preparar_uso_externo(
	relacao="_ambulatorio_atendimentos_resumo",
	cte_resultado="final"
) }}
SELECT * FROM final