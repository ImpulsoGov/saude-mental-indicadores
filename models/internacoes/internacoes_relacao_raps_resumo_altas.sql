{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_internacoes_relacao_raps_resumo_altas') }}

WITH
{{ preparar_uso_externo(
	relacao="_internacoes_relacao_raps_resumo_altas",
	cte_resultado="final"
) }}
SELECT * FROM final
