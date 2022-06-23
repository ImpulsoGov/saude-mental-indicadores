{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimento AS (
    SELECT * FROM {{ ref('atendimentos') }}
)
sexo AS (
    SELECT * FROM {{ source('sexos') }}
)
condicao_saude AS (
    SELECT * FROM {{ source('condicao_saude') }}
)
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    realizacao_periodo_data_inicio AS competencia,
    listas_de_codigos.nome_mes(realizacao_periodo_data_inicio) AS nome_mes,
    usuario_faixa_etaria,
    sexo.nome AS usuario_sexo,
    (
        CASE WHEN (condicao_principal_id_cid10 IS NULL) THEN 'Sem informação'
        ELSE coalesce(cid.cid_grupo_descricao_curta, 'Outras condições')
        END
    ) AS cid_grupo_descricao_curta,
    count(DISTINCT usuario_cns_criptografado) FILTER (
        WHERE quantidade_apresentada > 0
    ) AS usuarios_unicos_mes
FROM procedimento
FULL JOIN sexo
ON procedimento.usuario_sexo_id_sigtap = sexo.id_sigtap
LEFT JOIN condicao_saude
ON procedimento.condicao_principal_id_cid10 = condicao_saude.cid_id
GROUP BY
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    realizacao_periodo_data_inicio,
    usuario_faixa_etaria,
    sexo.nome,
    (condicao_principal_id_cid10 IS NULL),
    cid.cid_grupo_descricao_curta
