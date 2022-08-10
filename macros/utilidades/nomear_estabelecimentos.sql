{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro nomear_estabelecimentos(
    relacao,
    coluna_estabelecimento_nome="estabelecimento_nome",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id="0000000",
    todos_estabelecimentos_valor="Todos",
    cte_resultado="com_nomes_estabelecimentos"
) %}
{{ cte_resultado }} AS (
SELECT
    t.*,
    {% if todos_estabelecimentos_id is not none -%}
    (
        CASE
            WHEN
            {{ coluna_estabelecimento_id }} = '{{ todos_estabelecimentos_id }}'
            THEN '{{ todos_estabelecimentos_valor }}'
        ELSE
{%- endif %}    coalesce(
                estabelecimento.estabelecimento_nome_curto,
                estabelecimento.estabelecimento_nome
            )
    {%- if todos_sexos_id is not none -%}
        END
    ){%- endif %} AS {{ coluna_estabelecimento_nome}}
FROM {{ relacao }} t
LEFT JOIN (
    SELECT 
    {%- for coluna in adapter.get_columns_in_relation(
        source("codigos", "estabelecimentos")
    ) %}
        {{coluna.quoted}} AS "estabelecimento_{{coluna.name}}"
        {{- "," if not loop.last }}
    {%- endfor %}
    FROM {{ source("codigos", "estabelecimentos") }}
) estabelecimento
USING ({{ coluna_estabelecimento_id }})
)
{%- endmacro -%}
