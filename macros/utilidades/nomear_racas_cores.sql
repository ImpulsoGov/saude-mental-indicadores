{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro nomear_racas_cores(
    relacao,
    coluna_raca_cor_nome="raca_cor_nome",
    coluna_raca_cor_id="raca_cor_id_sigtap",
    todas_racas_cores_id="0",
    todas_racas_cores_valor="Todas",
    cte_resultado="com_nomes_racas_cores"
) %}
{{ cte_resultado }} AS (
SELECT
    t.*,
    {% if todas_racas_cores_id is not none -%}
    (
        CASE
            WHEN
            {{ coluna_raca_cor_id }} = '{{ todas_racas_cores_id }}'
            THEN '{{ todas_racas_cores_valor }}'
        ELSE
{%- endif %}    raca_cor.raca_cor_nome
    {%- if todas_racas_cores_id is not none -%}
        END
    ){%- endif %} AS {{ coluna_raca_cor_nome}}
FROM {{ relacao }} t
LEFT JOIN (
    SELECT 
    {%- for coluna in adapter.get_columns_in_relation(
        source("codigos", "racas_cores")
    ) %}
        {{coluna.quoted}} AS "raca_cor_{{coluna.name}}"
        {{- "," if not loop.last }}
    {%- endfor %}
    FROM {{ source("codigos", "racas_cores") }}
) raca_cor
USING ({{ coluna_raca_cor_id }})
)
{%- endmacro -%}
