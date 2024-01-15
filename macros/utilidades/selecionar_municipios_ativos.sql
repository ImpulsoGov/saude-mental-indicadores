{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


{%- macro 
    selecionar_municipios_ativos(
        relacao, 
        coluna_municipio_id="unidade_geografica_id_sus", 
        cte_resultado="municipios_selecionados"
    )
-%}

municipios_ativos AS (
	SELECT * FROM {{ source("utilidades", "municipios_painel_sm") }}
    WHERE status_ativo = TRUE
),
{{ cte_resultado }} AS (
    SELECT 
        tabela.*
    FROM {{ relacao }} tabela
    INNER JOIN municipios_ativos 
    ON tabela.{{ coluna_municipio_id }} = municipios_ativos.id_sus
)
{% endmacro %}