{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    revelar_combinacoes_implicitas(
        relacao,
        colunas_a_completar,
        agrupar_por=None,
        cte_resultado="com_combinacoes_vazias"
    )
-%}
{#############################################################################
    Para cada sequência de colunas a serem completas com combinações implicitas,
    selecionam-se as combinações únicas de categorias, considerando também as
    colunas informadas no argumento `agrupar_por`, se fornecido.

    Por exemplo, se uma sequência de colunas informadas na lista de
    `colunas_a_completar` é o par `['periodo_id', 'periodo_data_inicio']`,
    e as colunas informadas no argumento `agrupar_por` são
    `['unidade_geografica_id', 'unidade_geografica_id_sus']`, então neste
    trecho obtêm-se todas as combinações únicas de períodos existentes para
    cada unidade geografica existente na tabela de origem.
 #############################################################################}
{%- set agrupar_por_arg = '' if agrupar_por is none else agrupar_por %}
{%- for seq_colunas_a_completar in colunas_a_completar %}
{{ relacao }}_combinacoes_{{ loop.index }} AS (
SELECT DISTINCT
{%- if agrupar_por_arg %}
{%- for coluna in agrupar_por %}
    {{coluna}},
{%- endfor %}
{%- endif %}
{%- for coluna_a_completar in seq_colunas_a_completar %}
    {{ coluna_a_completar }}{{ "," if not loop.last }}
{%- endfor %}
FROM {{ relacao }}
),
{%- endfor %}
{#############################################################################
    As combinações existentes para cada conjunto de colunas separadamente
    são novamente combinadas entre si, usando o argumento `agrupar_por` para
    restringir as combinações válidas, se fornecido.
 #############################################################################}
{{ relacao }}_combinacoes AS (
SELECT
{%- if agrupar_por_arg %}
{%- for coluna in agrupar_por %}
    t1.{{coluna}},
{%- endfor %}
{%- endif %}
{%- for seq_colunas_a_completar in colunas_a_completar %}
{%- set loop_externo = loop -%}
{%- for coluna_a_completar in seq_colunas_a_completar %}
    t{{ loop_externo.index }}.{{ coluna_a_completar }}
    {{- "," if not loop.last or not loop_externo.last }}
{%- endfor %}
{%- endfor %}
{%- for seq_colunas_a_completar in colunas_a_completar %}
{{ "FROM " if loop.first else "CROSS JOIN " -}}
{{- relacao }}_combinacoes_{{ loop.index }} t{{ loop.index }}
{%- endfor %}
{% if agrupar_por_arg %}
WHERE
{%- for seq_colunas_a_completar in colunas_a_completar %}
{%- set loop_externo = loop %}
{%- for coluna in agrupar_por %}
{{ "    " if loop_externo.first and loop.first else "AND " -}}
t1.{{coluna}} = t{{ loop_externo.index }}.{{ coluna }}
{%- endfor %}
{%- endfor %}
{%- endif %}
),
{{ cte_resultado }} AS (
SELECT
    *    
FROM {{ relacao }}
FULL JOIN {{ relacao }}_combinacoes
USING (
{%- if agrupar_por_arg %}
{%- for coluna in agrupar_por %}
    {{ coluna }},
{%- endfor %}
{%- endif %}
{%- for seq_colunas_a_completar in colunas_a_completar %}
{%- set loop_externo = loop %}
{%- for coluna in seq_colunas_a_completar %}
    {{coluna}}{{ "," if not (loop.last and loop_externo.last) }}
{%- endfor %}
{%- endfor %}
)
)
{%- endmacro -%}
