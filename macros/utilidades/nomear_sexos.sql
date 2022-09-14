{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro nomear_sexos(
    relacao,
    coluna_sexo_nome="sexo_nome",
    coluna_sexo_id="sexo_id_sigtap",
    todos_sexos_id="0",
    todos_sexos_valor="Todos",
    cte_resultado="com_nomes_sexos"
) %}
{%- set re = modules.re -%}
{%- set lista_de_codigos_colunas = adapter.get_columns_in_relation(
    source("codigos", "sexos")
) -%}
{%- set lista_de_codigos_coluna_id = (
    "sexo" + re.match(".*sexo.*(_id.*)", coluna_sexo_id).groups(1)[0]
) -%}
{{ cte_resultado }} AS (
SELECT
    t.*,
    {% if todos_sexos_id is not none -%}
    (
        CASE
            WHEN
            t.{{ coluna_sexo_id }} = '{{ todos_sexos_id }}'
            THEN '{{ todos_sexos_valor }}'
        ELSE
{%- endif %}    sexo.sexo_nome
    {%- if todos_sexos_id is not none -%}
        END
    ){%- endif %} AS {{ coluna_sexo_nome }}
FROM {{ relacao }} t
LEFT JOIN (
    SELECT 
    {%- for coluna in lista_de_codigos_colunas %}
        {{coluna.quoted}} AS "sexo_{{coluna.name}}"
        {{- "," if not loop.last }}
    {%- endfor %}
    FROM {{ source("codigos", "sexos") }}
) sexo
ON t.{{ coluna_sexo_id }} = sexo.{{ lista_de_codigos_coluna_id }}
)
{%- endmacro -%}
