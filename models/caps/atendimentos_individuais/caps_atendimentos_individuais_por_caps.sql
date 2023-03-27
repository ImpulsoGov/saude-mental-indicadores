{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
{{ preparar_uso_externo(
	relacao="_caps_atendimentos_individuais_por_caps",
	cte_resultado="final"
) }}
SELECT * FROM final