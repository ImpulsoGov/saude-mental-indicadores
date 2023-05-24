{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro remover_subtotais(
    relacao,
    cte_resultado="sem_subtotais"
) %}
{%- set re = modules.re -%}
sem_todos AS (
    SELECT
        t.*
    FROM {{ relacao }} t
    WHERE estabelecimento_id_scnes <> '0000000' 
        AND estabelecimento_linha_idade <> 'Todos' 
        AND estabelecimento_linha_perfil <> 'Todos'
),
apenas_todos AS (
    SELECT
        t.*
    FROM {{ relacao }} t
    WHERE estabelecimento_id_scnes = '0000000' 
        AND estabelecimento_linha_idade = 'Todos' 
        AND estabelecimento_linha_perfil = 'Todos' 
),
{{ cte_resultado }} AS (
    SELECT * FROM apenas_todos
    UNION ALL
    SELECT * FROM sem_todos
)
{%- endmacro -%}
