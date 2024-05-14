{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

WITH
estabelecimentos_ausentes AS (
    SELECT * FROM {{ ref("_configuracoes_estabelecimentos_ausentes_por_periodos") }}
),

raas_psicossocial_disseminacao AS (
    SELECT 
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio
    FROM {{ source("siasus", "raas_psicossocial_disseminacao") }}
),

raas_ultimo_periodo AS (
    SELECT DISTINCT ON (LEFT(unidade_geografica_id_sus, 2), realizacao_periodo_data_inicio) 
        LEFT(unidade_geografica_id_sus, 2) as unidade_geografica_id_sus, 
        realizacao_periodo_data_inicio as periodo_data_inicio,
        CASE
            WHEN realizacao_periodo_data_inicio = max_periodo THEN 'Último período'
            ELSE (CASE
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 1 THEN 'Jan/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 2 THEN 'Fev/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 3 THEN 'Mar/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 4 THEN 'Abr/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 5 THEN 'Mai/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 6 THEN 'Jun/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 7 THEN 'Jul/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 8 THEN 'Ago/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 9 THEN 'Set/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 10 THEN 'Out/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 11 THEN 'Nov/'
                WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 12 THEN 'Dez/'
            END || to_char(realizacao_periodo_data_inicio, 'YY')) 
        END AS periodo,
        CASE
            WHEN realizacao_periodo_data_inicio = max_periodo_adesao THEN 'Último período'
            WHEN realizacao_periodo_data_inicio < max_periodo_adesao THEN
                (CASE
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 1 THEN 'Jan/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 2 THEN 'Fev/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 3 THEN 'Mar/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 4 THEN 'Abr/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 5 THEN 'Mai/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 6 THEN 'Jun/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 7 THEN 'Jul/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 8 THEN 'Ago/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 9 THEN 'Set/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 10 THEN 'Out/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 11 THEN 'Nov/'
                    WHEN EXTRACT(MONTH FROM realizacao_periodo_data_inicio) = 12 THEN 'Dez/'
                END || to_char(realizacao_periodo_data_inicio, 'YY')) 
            ELSE NULL
        END AS periodo_adesao
    FROM (
        SELECT distinct on (LEFT(unidade_geografica_id_sus, 2), realizacao_periodo_data_inicio) 
            LEFT(unidade_geografica_id_sus, 2) as unidade_geografica_id_sus, 
            realizacao_periodo_data_inicio,
            MAX(realizacao_periodo_data_inicio) OVER () AS max_periodo,
            MAX(realizacao_periodo_data_inicio) OVER () - INTERVAL '4 months' AS max_periodo_adesao
        FROM raas_psicossocial_disseminacao
    ) AS subquery
),

{{ classificar_caps_linha(
    relacao="estabelecimentos_ausentes",
    cte_resultado="com_linhas_estabelecimentos"
) }},

{{ nomear_estabelecimentos(
    relacao="com_linhas_estabelecimentos",
    coluna_estabelecimento_nome="estabelecimento",
    cte_resultado="com_nomes_estabelecimentos"
) }},

com_datas_legiveis AS (
    SELECT 
        ea.*,
        (
            CASE 
                WHEN tabela_referencia = 'caps_adesao_usuarios_perfil_cid' THEN rup.periodo_adesao
                WHEN tabela_referencia = 'caps_adesao_evasao_coortes_resumo' THEN rup.periodo_adesao
                ELSE rup.periodo
            END
        ) AS periodo,
        (
            CASE
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 1 THEN 'Janeiro'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 2 THEN 'Fevereiro'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 3 THEN 'Março'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 4 THEN 'Abril'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 5 THEN 'Maio'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 6 THEN 'Junho'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 7 THEN 'Julho'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 8 THEN 'Agosto'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 9 THEN 'Setembro'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 10 THEN 'Outubro'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 11 THEN 'Novembro'
                WHEN EXTRACT ( MONTH FROM ea.periodo_data_inicio ) = 12 THEN 'Dezembro'
            END
        ) AS nome_mes,
        (
            to_char(ea.periodo_data_inicio, 'YY')::numeric + EXTRACT ( MONTH FROM ea.periodo_data_inicio )/100
        ) AS periodo_ordem
    FROM com_nomes_estabelecimentos ea
    LEFT JOIN raas_ultimo_periodo rup
    ON (LEFT(ea.unidade_geografica_id_sus, 2)) = rup.unidade_geografica_id_sus
    AND ea.periodo_data_inicio = rup.periodo_data_inicio
),

final AS (
    SELECT
        id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_linha_perfil,
        estabelecimento_linha_idade,
        estabelecimento,
        periodo_id,
        periodo_data_inicio AS competencia,
        periodo,
        nome_mes,
        periodo_ordem,        
        tabela_referencia,
        atualizacao_data
    FROM com_datas_legiveis
)

SELECT * FROM final
