{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
{{ preparar_uso_externo(
	relacao="_reducao_danos_acoes_por_estabelecimento_12meses",
	cte_resultado="final"
) }}
SELECT * FROM final