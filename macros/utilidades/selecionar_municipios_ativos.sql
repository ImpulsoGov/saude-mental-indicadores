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

{% set usar_municipio_id = coluna_municipio_id == "unidade_geografica_id" %}

municipios_ativos AS (
	SELECT * FROM {{ source("utilidades", "municipios_painel_sm") }}
    WHERE status_ativo = TRUE
),
{{ cte_resultado }} AS (
    SELECT 
        tabela.*
    FROM {{ relacao }} tabela
    INNER JOIN municipios_ativos 

    {% if usar_municipio_id %}
        ON tabela.{{ coluna_municipio_id }} = municipios_ativos.id_mun
    {% else %}
        ON tabela.{{ coluna_municipio_id }} = municipios_ativos.id_sus
    {% endif %}  
)

{% endmacro %}