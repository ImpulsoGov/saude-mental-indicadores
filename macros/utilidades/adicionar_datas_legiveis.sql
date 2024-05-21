{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro adicionar_datas_legiveis(
    relacao,
    coluna_periodo_data="periodo_data_inicio",
    prefixo_colunas="",
    cte_resultado="com_datas_legiveis",
    tags=[]
) %}

{%- set mes -%}
EXTRACT ( MONTH FROM {{ coluna_periodo_data }} )
{%- endset -%}


{%- if tags is defined and 'caps_uso_externo' in tags %}

{{ coluna_periodo_data -}}_ultimas_competencias AS (
    SELECT
        DISTINCT ON (t.unidade_geografica_id, t.unidade_geografica_id_sus, t.estabelecimento)
        t.unidade_geografica_id,
        t.unidade_geografica_id_sus,
		t.estabelecimento,
        {{ coluna_periodo_data }} AS ultima_competencia
    FROM {{ relacao }} t       
    LEFT JOIN (
        SELECT 
            unidade_geografica_id_sus, estabelecimento
        FROM {{ ref("configuracoes_estabelecimentos_ausentes_por_periodos") }}

    {%- if tags is defined and 'usuarios_ativos' in tags %}
            WHERE tabela_referencia = 'caps_usuarios_ativos_perfil_condicao_semsubtotais'
    {%- endif -%}

    {%- if tags is defined and 'usuarios_novos' in tags %}
            WHERE tabela_referencia = 'caps_usuarios_novos_perfil_condicao'
    {%- endif -%}

    {%- if tags is defined and 'atendimentos_individuais' in tags %}
            WHERE tabela_referencia = 'caps_usuarios_atendimentos_individuais_perfil_cid'
    {%- endif -%}

    {%- if tags is defined and 'adesao_mensal' in tags %}
            WHERE tabela_referencia = 'caps_adesao_usuarios_perfil_cid' 
    {%- endif -%}

    {%- if tags is defined and 'adesao_acumulada' in tags %}
            WHERE tabela_referencia = 'caps_adesao_evasao_coortes_resumo'
    {%- endif -%}

    {%- if tags is defined and 'procedimentos_por_usuario' in tags %}
            WHERE tabela_referencia = 'caps_procedimentos_por_usuario_por_tempo_servico'
    {%- endif -%}

    {%- if tags is defined and 'procedimentos_por_hora' in tags %}
            WHERE tabela_referencia = 'caps_procedimentos_por_hora_resumo'
    {%- endif -%}

    {%- if tags is defined and 'procedimentos_por_tipo' in tags %}
            WHERE tabela_referencia = 'caps_procedimentos_por_tipo'
    {%- endif -%}

        AND periodo = 'Último período'
        ) AS tr
    ON t.unidade_geografica_id_sus = tr.unidade_geografica_id_sus 
        AND t.estabelecimento = tr.estabelecimento     	
    WHERE tr.unidade_geografica_id_sus IS NULL    	
    ORDER BY
    	unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento,
        {{ coluna_periodo_data }} DESC
),

{%- else -%}
    {{ coluna_periodo_data -}}_ultimas_competencias AS (
        SELECT
            DISTINCT ON (unidade_geografica_id)
            unidade_geografica_id,
            {{ coluna_periodo_data }} AS ultima_competencia
        FROM {{ relacao }}
        ORDER BY
            unidade_geografica_id,
            {{ coluna_periodo_data }} DESC
    ),
{%- endif %}


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
    LEFT JOIN {{ coluna_periodo_data -}}_ultimas_competencias uc
{% if tags is defined and ('usuarios_ativos' in tags or 'usuarios_novos' in tags or 'atendimentos_individuais' in tags or 'adesao_mensal' in tags or 'adesao_acumulada' in tags or 'procedimentos_por_usuario' in tags or 'procedimentos_por_hora' in tags or 'procedimentos_por_tipo' in tags) -%}
    ON t.unidade_geografica_id = uc.unidade_geografica_id AND t.estabelecimento = uc.estabelecimento 
{% else -%}
    USING (unidade_geografica_id)
{%- endif -%}    
)
{%- endmacro -%}
