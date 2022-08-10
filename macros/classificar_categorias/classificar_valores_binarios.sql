{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    classificar_valores_binarios(
        coluna_valores,
        valor_verdadeiro="Sim",
        valor_falso="Não",
        valor_nulo="Sem informação"
    )
-%}
(
    CASE
        WHEN {{ coluna_valores }} IS NULL THEN RETURN {{ valor_nulo }};
        WHEN {{ coluna_valores }} THEN RETURN {{ valor_verdadeiro }};
        ELSE RETURN {{ valor_falso }};
    END
)
{%- endmacro -%}
