{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_consultorio_na_rua_atendimentos') }}

WITH
{{ preparar_uso_externo(
	relacao="_consultorio_na_rua_atendimentos",
	cte_resultado="final"
) }}
SELECT * FROM final