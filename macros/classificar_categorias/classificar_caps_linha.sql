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
    todas_linhas_valor="Todas",
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
                    estabelecimento.estabelecimento_nome LIKE '%Ad %'
                    OR estabelecimento.estabelecimento_nome LIKE '%Alc%'
                    OR estabelecimento.estabelecimento_nome LIKE '%lcool%'
                    OR estabelecimento.estabelecimento_nome LIKE '%Drg%'
                    OR estabelecimento.estabelecimento_nome LIKE '%Droga%'
                    -- TODO: gambiarra horrivel, mudar!!!
                    OR estabelecimento.estabelecimento_nome
                        = 'Caps Era Uma Vez'
                THEN 'Álcool e outras drogas'
                ELSE 'Transtornos'
            END
        ) AS {{ coluna_linha_perfil }},
        (
            CASE
                WHEN
                    t.{{ coluna_estabelecimento_id }}
                    = '{{todos_estabelecimentos_id}}'
                THEN '{{ todas_linhas_valor }}'
                WHEN
                    estabelecimento.estabelecimento_nome LIKE '%Inf%'
                    OR estabelecimento.estabelecimento_nome LIKE '%nfant%'
                    OR estabelecimento.estabelecimento_nome LIKE '%Juven%'
                    OR estabelecimento.estabelecimento_nome LIKE '%Capsi%'
                    -- TODO: gambiarra horrivel, mudar!!!
                    OR estabelecimento.estabelecimento_nome
                        = 'Caps Era Uma Vez'
                THEN 'Infantil/Infanto-Juvenil'
                ELSE 'Adulto'
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
