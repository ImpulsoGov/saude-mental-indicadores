{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro classificar_caps_linha(
    relacao,
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id="0000000",
    todas_linhas_valor="Todos",
    cte_resultado="com_linhas_cuidado"
) %}
{%- set re = modules.re -%}
{%- set lista_de_codigos_colunas = adapter.get_columns_in_relation(
    source("codigos", "estabelecimentos")
) -%}
{%- set lista_de_codigos_coluna_id = "estabelecimento" + re.match(
        ".*estabelecimento.*(_id.*)",
        coluna_estabelecimento_id
    ).groups(1)[0]
 -%}
{{ cte_resultado }} AS (
    SELECT
        t.*,
        (
            CASE
            {%- if todos_estabelecimentos_id is not none %}
                WHEN
                    t.{{ coluna_estabelecimento_id }}
                    = '{{todos_estabelecimentos_id}}'
                THEN '{{ todas_linhas_valor }}'
            {%- endif %}
                WHEN
                    estabelecimento.estabelecimento_linha_perfil = 'Álcool e outras drogas'
                THEN 'Álcool e outras drogas'
                WHEN
                    estabelecimento.estabelecimento_linha_perfil = 'Geral'
                THEN 'Geral'
                ELSE 'Sem classificação'
            END
        ) AS {{ coluna_linha_perfil }},
        (
            CASE
                WHEN
                    t.{{ coluna_estabelecimento_id }}
                    = '{{todos_estabelecimentos_id}}'
                THEN '{{ todas_linhas_valor }}'
                WHEN
                    estabelecimento.estabelecimento_linha_idade = 'Infantil/Infanto-Juvenil'
                THEN 'Infantil/Infanto-Juvenil'
                WHEN
                    estabelecimento.estabelecimento_linha_idade = 'Adulto'
                THEN 'Adulto'
                WHEN
                    estabelecimento.estabelecimento_linha_idade = 'Adulto + Infantil/Infanto-Juvenil (+15a)'
                THEN 'Adulto e Juvenil (+15a)'
                WHEN
                    estabelecimento.estabelecimento_linha_idade = 'Adulto + Infantil/Infanto-Juvenil'
                THEN 'Adulto e Infantil/Infanto-Juvenil'
                ELSE 'Sem classificação'
            END
        ) AS {{ coluna_linha_idade }}
    FROM {{ relacao }} t
    LEFT JOIN (
        SELECT 
        {%- for coluna in lista_de_codigos_colunas %}
            {{coluna.quoted}} AS "estabelecimento_{{coluna.name}}"
            {{- "," if not loop.last }}
        {%- endfor %}
        FROM {{ source("codigos", "estabelecimentos") }}
    ) estabelecimento
    ON 
        t.{{ coluna_estabelecimento_id }}
        = estabelecimento.{{ lista_de_codigos_coluna_id }}
)
{%- endmacro -%}
