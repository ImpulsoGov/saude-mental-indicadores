{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_ativos_por_estabelecimento_resumo AS (
    SELECT * FROM {{ ref("caps_usuarios_ativos_por_estabelecimento_resumo") }}
),
{{ ultimas_competencias(
    relacao="usuarios_ativos_por_estabelecimento_resumo",
    fontes=[
		"raas_psicossocial_disseminacao",
		"bpa_i_disseminacao"
	],
    meses_antes_ultima_competencia=(0, 0),
    cte_resultado="final"
) }}
SELECT * FROM final
