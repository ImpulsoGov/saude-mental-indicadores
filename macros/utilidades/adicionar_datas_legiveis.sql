{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro adicionar_datas_legiveis(
    relacao,
    coluna_periodo_data="periodo_data_inicio",
    prefixo_colunas="",
    cte_resultado="com_datas_legiveis"
) %}
{%- set mes -%}
EXTRACT ( MONTH FROM {{ coluna_periodo_data }} )
{%- endset -%}
{{ coluna_periodo_data -}}_ultimas_competencias AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        {{ coluna_periodo_data }} AS ultima_competencia
    FROM {{ relacao }}
    ORDER BY
        unidade_geografica_id,
        {{ coluna_periodo_data }} DESC
),
{{ cte_resultado }} AS (
    SELECT
        t.*,
        (
            CASE
                WHEN {{ coluna_periodo_data }} = ultima_competencia
                THEN 'Último período'
            ELSE (
                CASE
                    WHEN {{ mes }} = 1 THEN 'Jan/'
                    WHEN {{ mes }} = 2 THEN 'Fev/'
                    WHEN {{ mes }} = 3 THEN 'Mar/'
                    WHEN {{ mes }} = 4 THEN 'Abr/'
                    WHEN {{ mes }} = 5 THEN 'Mai/'
                    WHEN {{ mes }} = 6 THEN 'Jun/'
                    WHEN {{ mes }} = 7 THEN 'Jul/'
                    WHEN {{ mes }} = 8 THEN 'Ago/'
                    WHEN {{ mes }} = 9 THEN 'Set/'
                    WHEN {{ mes }} = 10 THEN 'Out/'
                    WHEN {{ mes }} = 11 THEN 'Nov/'
                    WHEN {{ mes }} = 12 THEN 'Dez/'
                END || to_char({{ coluna_periodo_data }}, 'YY')
            ) END
        )  AS {{ prefixo_colunas -}}periodo,
        (
            CASE
                WHEN {{ mes }} = 1 THEN 'Janeiro'
                WHEN {{ mes }} = 2 THEN 'Fevereiro'
                WHEN {{ mes }} = 3 THEN 'Março'
                WHEN {{ mes }} = 4 THEN 'Abril'
                WHEN {{ mes }} = 5 THEN 'Maio'
                WHEN {{ mes }} = 6 THEN 'Junho'
                WHEN {{ mes }} = 7 THEN 'Julho'
                WHEN {{ mes }} = 8 THEN 'Agosto'
                WHEN {{ mes }} = 9 THEN 'Setembro'
                WHEN {{ mes }} = 10 THEN 'Outubro'
                WHEN {{ mes }} = 11 THEN 'Novembro'
                WHEN {{ mes }} = 12 THEN 'Dezembro'
            END
        ) AS {{ prefixo_colunas -}}nome_mes,
        (
            to_char({{ coluna_periodo_data }}, 'YY')::numeric + {{ mes }}/100
        ) AS {{ prefixo_colunas -}}periodo_ordem
    FROM {{ relacao }} t
    LEFT JOIN {{ coluna_periodo_data -}}_ultimas_competencias
    USING (unidade_geografica_id)
)
{%- endmacro -%}
