{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimento AS (
    SELECT * FROM {{ ref('ambulatorio_atendimentos') }}
),
condicao_saude AS (
    SELECT * FROM {{ source('codigos', 'condicoes_saude') }}
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id",
            "estabelecimento_id_scnes",
            "faixa_etaria_id",
            "usuario_sexo_id_sigtap",
            "condicao_saude.grupo_id_cid10"
        ])}} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        estabelecimento_id_scnes,
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        faixa_etaria_descricao AS usuario_faixa_etaria,
        faixa_etaria_ordem AS usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        (
            CASE
            WHEN (condicao_principal_id_cid10 IS NULL) THEN 'Sem informação'
            ELSE condicao_saude.grupo_descricao_curta_cid10
            END
        ) AS cid_grupo_descricao_curta,
        count(DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE quantidade_apresentada > 0
        ) AS usuarios_unicos_mes,
        now() AS atualizacao_data
    FROM procedimento
    LEFT JOIN condicao_saude
    ON procedimento.condicao_principal_id_cid10 = condicao_saude.id_cid10
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        faixa_etaria_id,
        faixa_etaria_descricao,
        faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
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
