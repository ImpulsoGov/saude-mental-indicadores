{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    classificar_coluna_binaria(
        coluna,
        classe_verdadeiro="Sim",
        classe_falso="Não",
        classe_nulo="Sem informação"
    )
-%}
(
    CASE
        WHEN {{ coluna }} IS NULL THEN 'Sem informação'
        WHEN {{ coluna }} THEN 'Sim'
        ELSE 'Não'
    END
)
{%- endmacro -%}
