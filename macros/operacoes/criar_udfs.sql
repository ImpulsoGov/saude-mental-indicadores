{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro criar_udfs() -%}

{%- do run_query(diferenca_dias_uteis()) %};

{%- do run_query(proximo_dia_util()) %};

{%- endmacro -%}
