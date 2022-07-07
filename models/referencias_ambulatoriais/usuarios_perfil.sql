{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimento AS (
    SELECT * FROM {{ ref('atendimentos') }}
),
sexo AS (
    SELECT * FROM {{ source('codigos', 'sexos') }}
),
condicao_saude AS (
    SELECT * FROM {{ source('codigos', 'condicoes_saude') }}
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id",
            "faixa_etaria_id",
            "sexo.id",
            "condicao_saude.grupo_id_cid10"
        ])}} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio AS competencia,
        listas_de_codigos.nome_mes(realizacao_periodo_data_inicio) AS nome_mes,
        faixa_etaria_descricao AS usuario_faixa_etaria,
        faixa_etaria_ordem AS usuario_faixa_etaria_ordem,
        sexo.nome AS usuario_sexo,
        (
            CASE
            WHEN (condicao_principal_id_cid10 IS NULL) THEN 'Sem informação'
            ELSE condicao_saude.grupo_descricao_curta_cid10
            END
        ) AS cid_grupo_descricao_curta,
        count(DISTINCT usuario_cns_criptografado) FILTER (
            WHERE quantidade_apresentada > 0
        ) AS usuarios_unicos_mes,
        now() AS atualizacao_data
    FROM procedimento
    FULL JOIN sexo
    ON procedimento.usuario_sexo_id_sigtap = sexo.id_sigtap
    LEFT JOIN condicao_saude
    ON procedimento.condicao_principal_id_cid10 = condicao_saude.id_cid10
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio,
        faixa_etaria_id,
        faixa_etaria_descricao,
        faixa_etaria_ordem,
        sexo.id,
        sexo.nome,
        (condicao_principal_id_cid10 IS NULL),
        condicao_saude.grupo_id_cid10,
        condicao_saude.grupo_descricao_curta_cid10
    {% if is_incremental() %}
    HAVING max(procedimento.atualizacao_data) > (
        SELECT max(atualizacao_data) FROM {{this}}
    )
    {% endif %}
)
SELECT * FROM final
