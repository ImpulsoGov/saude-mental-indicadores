{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_encaminhamentos_aps_caps') }}

WITH
{{ preparar_uso_externo(
	relacao="_encaminhamentos_aps_caps",
	cte_resultado="final"
) }}
SELECT * FROM final