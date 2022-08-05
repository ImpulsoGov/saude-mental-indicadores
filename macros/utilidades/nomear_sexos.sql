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
{{ cte_resultado }} AS (
SELECT
    t.*,
    {% if todos_sexos_id is not none -%}
    (
        CASE
            WHEN
            {{ coluna_sexo_id }} = '{{ todos_sexos_id }}'
            THEN '{{ todos_sexos_valor }}'
        ELSE
{%- endif %}    sexo.sexo_nome
    {%- if todos_sexos_id is not none -%}
        END
    ){%- endif %} AS {{ coluna_sexo_nome}}
FROM {{ relacao }} t
LEFT JOIN (
    SELECT 
    {%- for coluna in adapter.get_columns_in_relation(
        source("codigos", "sexos")
    ) %}
        {{coluna.quoted}} AS "sexo_{{coluna.name}}"
        {{- "," if not loop.last }}
    {%- endfor %}
    FROM {{ source("codigos", "sexos") }}
) sexo
USING ({{ coluna_sexo_id }})
)
{%- endmacro -%}
