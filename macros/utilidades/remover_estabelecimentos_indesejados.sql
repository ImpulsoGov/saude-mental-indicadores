{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


{%- macro 
    remover_estabelecimentos_indesejados(
        relacao, 
        coluna_estabelecimento_id="estabelecimento_id_scnes", 
        cte_resultado="municipios_selecionados"
    )
-%}

estabelecimentos_a_remover AS (
	SELECT * FROM {{ source("codigos", "estabelecimentos") }}
    WHERE remover_painel = TRUE
),

{{ cte_resultado }} AS (
    SELECT 
        tabela.*
    FROM {{ relacao }} tabela
    LEFT JOIN estabelecimentos_a_remover ear 
    ON tabela.{{ coluna_estabelecimento_id }} = ear.id_scnes
    WHERE ear.id_scnes IS NULL
)

{% endmacro %}

