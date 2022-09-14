{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}



{%- macro filtrar_regex(iteravel, expressao_regular) -%}
{% set re = modules.re %}
{% set filtrado = [] %}
{%- for texto in iteravel -%}
{%- if re.match(expressao_regular, texto) -%}
{%- set _ = filtrado.append(texto) -%}
{%- endif -%}
{%- endfor -%}
{{- return(filtrado) -}}
{%- endmacro -%}