{#
SPDX-FileCopyrightText: 2023 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_adesao_evasao_coortes') }}

WITH
{{ preparar_uso_externo(
	relacao="_caps_adesao_evasao_coortes",
	cte_resultado="final"
) }}
SELECT * FROM final
