{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
{{ preparar_uso_externo(
	relacao="_caps_usuarios_ativos_perfil_cidcompleto_barbalha",
	cte_resultado="final"
) }}
SELECT * FROM final
