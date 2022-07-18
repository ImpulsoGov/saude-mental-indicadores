{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    totalizar_coluna(
        relacao,
        agrupar_por,
        coluna_a_totalizar,
        nome_categoria_com_totais,
        agregacoes_valores,
        cte_resultado="_subtotal"
    )
-%}
{{ cte_resultado }} AS (
SELECT 
{%- for coluna in agrupar_por %}
    {{ coluna }},
{%- endfor %}
    '{{ nome_categoria_com_totais }}' AS {{ coluna_a_totalizar }},
{%- for coluna, agregacao in agregacoes_valores.items() %}
    {{ agregacao }}({{ coluna }}) AS {{ coluna }}{{ "," if not loop.last }}
{%- endfor %}
FROM {{ relacao }}
GROUP BY
{%- for coluna in agrupar_por %}
    {{ coluna }}{{ "," if not loop.last }}
{%- endfor %}
)
{%- endmacro -%}
