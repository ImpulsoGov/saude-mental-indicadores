{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_encaminhamentos_aps_especializada') }}

WITH
{{ preparar_uso_externo(
	relacao="_encaminhamentos_aps_especializada",
	cte_resultado="final"
) }}
SELECT * FROM final