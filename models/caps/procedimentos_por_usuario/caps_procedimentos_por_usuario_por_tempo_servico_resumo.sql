{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimentos_por_tempo_servico AS (
    SELECT * FROM {{ ref("caps_procedimentos_por_usuario_por_tempo_servico") }}
),
final AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id,
            unidade_geografica_id_sus,
            estabelecimento
        )
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes,
        estabelecimento,
        tempo_servico_descricao AS tempo_servico_maior_taxa,
        procedimentos_por_usuario AS maior_taxa
    FROM procedimentos_por_tempo_servico
    ORDER BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento,
        competencia DESC,
        procedimentos_por_usuario DESC
)
SELECT * FROM final