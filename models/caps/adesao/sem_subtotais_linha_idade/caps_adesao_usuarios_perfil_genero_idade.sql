{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_caps_adesao_usuarios_perfil_genero_idade') }}

WITH
{{ preparar_uso_externo(
	relacao="_caps_adesao_usuarios_perfil_genero_idade",
	cte_resultado="final"
) }}
SELECT * FROM final
