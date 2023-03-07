{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
procedimentos_disseminacao AS (
    SELECT * FROM {{ source('siasus', 'procedimentos_disseminacao') }}
),
ultima_competencia AS (
    SELECT * FROM {{ ref('procedimentos_disseminacao_ultima_competencia') }}
),
procedimentos_ano AS (
    SELECT 
        procedimentos_disseminacao.*,
        ultima_competencia.periodo_data_inicio AS competencia
    FROM 
        procedimentos_disseminacao
    INNER JOIN
        ultima_competencia
    ON
        procedimentos_disseminacao.unidade_geografica_id = ultima_competencia.unidade_geografica_id
        AND date_part('year', procedimentos_disseminacao.realizacao_periodo_data_inicio) 
        = date_part('year', ultima_competencia.periodo_data_inicio)
),
matriciamentos_por_caps_ultimo_ano AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        max(competencia) AS competencia,
        estabelecimento_id_scnes,
        date_part('month', max(competencia)) AS meses_decorridos,
        sum(quantidade_apresentada) FILTER (
            WHERE procedimento_id_sigtap = '0301080305'
        ) AS quantidade_registrada
    FROM procedimentos_ano
    WHERE
        estabelecimento_tipo_id_sigtap = '70'
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes
),
final AS (
    SELECT
        matriciamento.unidade_geografica_id,
        matriciamento.unidade_geografica_id_sus,
        matriciamento.competencia,
        date_part('year', matriciamento.competencia)::text AS ano,
        listas_de_codigos.nome_mes(matriciamento.competencia) AS ate_mes,
        matriciamento.estabelecimento_id_scnes,
        coalesce(matriciamento.quantidade_registrada, 0) AS quantidade_registrada,
        greatest(
            0,
            12 - coalesce(matriciamento.quantidade_registrada, 0)
        ) AS faltam_no_ano,
        round(
            (
                greatest(0, 12 - coalesce(matriciamento.quantidade_registrada, 0))
                / nullif(12 - matriciamento.meses_decorridos, 0)::numeric
            ),
            1
        ) AS media_mensal_para_meta
    FROM matriciamentos_por_caps_ultimo_ano matriciamento
)
SELECT * FROM final