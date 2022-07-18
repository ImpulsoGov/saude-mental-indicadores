{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    revelar_combinacoes_implicitas(
        relacao,
        agrupar_por,
        colunas_a_completar,
        cte_resultado="com_combinacoes_vazias"
    )
-%}
{#############################################################################
    Para cada sequência de colunas a serem completas com combinações implicitas,
    selecionam-se as combinações únicas de categorias, considerando também as
    colunas informadas no argumento `agrupar_por`.

    Por exemplo, se uma sequência de colunas informadas na lista de
    `colunas_a_completar` é o par `['periodo_id', 'periodo_data_inicio']`,
    e as colunas informadas no argumento `agrupar_por` são
    `['unidade_geografica_id', 'unidade_geografica_id_sus']`, então neste
    trecho obtêm-se todas as combinações únicas de períodos existentes para
    cada unidade geografica existente na tabela de origem.
 #############################################################################}
{%- for seq_colunas_a_completar in colunas_a_completar %}
{{ relacao }}_combinacoes_{{ loop.index }} AS (
SELECT DISTINCT
{%- for coluna in agrupar_por %}
    {{coluna}},
{%- endfor %}
{%- for coluna_a_completar in seq_colunas_a_completar %}
    {{ coluna_a_completar }}{{ "," if not loop.last }}
{%- endfor %}
FROM {{ relacao }}
),
{%- endfor %}
{#############################################################################
    As combinações existentes para cada conjunto de calounas separadamente
    são novamente combinadas entre si, usando o argumento `agrupar_por` para
    restringir as combinações válidas.
 #############################################################################}
{{ relacao }}_combinacoes AS (
SELECT
{%- for coluna in agrupar_por %}
    t1.{{coluna}},
{%- endfor %}
{%- for seq_colunas_a_completar in colunas_a_completar %}
{%- set loop_externo = loop -%}
{%- for coluna_a_completar in seq_colunas_a_completar %}
    t{{ loop_externo.index }}.{{ coluna_a_completar }}
    {{- "," if not loop_externo.last }}
{%- endfor %}
{%- endfor %}
{%- for seq_colunas_a_completar in colunas_a_completar %}
{{ "FROM " if loop.first else "CROSS JOIN " -}}
{{- relacao }}_combinacoes_{{ loop.index }} t{{ loop.index }}
{%- endfor %}
WHERE
{%- for seq_colunas_a_completar in colunas_a_completar %}
{%- set loop_externo = loop %}
{%- for coluna in agrupar_por %}
{{ "    " if loop_externo.first and loop.first else "AND " -}}
t1.{{coluna}} = t{{ loop_externo.index }}.{{ coluna }}
{%- endfor %}
{%- endfor %}
),
{{ cte_resultado }} AS (
SELECT
    *    
FROM {{ relacao }}
FULL JOIN {{ relacao }}_combinacoes
USING (
{%- for coluna in agrupar_por %}
    {{ coluna }},
{%- endfor %}
{%- for seq_colunas_a_completar in colunas_a_completar %}
{%- set loop_externo = loop %}
{%- for coluna in seq_colunas_a_completar %}
    {{coluna}}{{ "," if not (loop.last and loop_externo.last) }}
{%- endfor %}
{%- endfor %}
)
)
{%- endmacro -%}
