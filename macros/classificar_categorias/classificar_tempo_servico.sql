{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro 
    classificar_tempo_servico(
        relacao,
        coluna_primeiro_procedimento,
        coluna_data_referencia,
        cte_resultado="com_tempo_servico"
    )
-%}
{%- set intervalo -%}
    age({{ coluna_data_referencia }}, {{ coluna_primeiro_procedimento }})
{%- endset -%}
intervalos_tempo_servico AS (
    SELECT * FROM {{ ref("intervalos_tempo_servico") }}
),
{{ cte_resultado }} AS (
    SELECT 
        t.*,
        tempo_servico.id AS tempo_servico_id,
        tempo_servico.descricao AS tempo_servico_descricao,
        tempo_servico.ordem AS tempo_servico_ordem
    FROM {{ relacao }} t
    LEFT JOIN intervalos_tempo_servico tempo_servico
    ON ({{ intervalo }} >= tempo_servico.minimo)
    AND ({{ intervalo }} < tempo_servico.maximo)
)
{%- endmacro -%}
