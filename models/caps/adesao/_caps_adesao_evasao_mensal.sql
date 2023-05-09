{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
perfil_usuarios_por_adesao AS (
    SELECT * FROM {{ ref("_caps_adesao_usuarios_perfil") }}
),

evadiram_vs_total AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        sum(quantidade_registrada) FILTER (
            WHERE estatus_adesao_mes = 'Evadiram no mês'
        ) AS usuarios_recentes_evadiram,
        sum(quantidade_registrada) AS usuarios_recentes_total,
        max(atualizacao_data) AS atualizacao_data
    FROM perfil_usuarios_por_adesao
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes
),

final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id_sus",
            "estabelecimento_id_scnes",
            "periodo_id"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        round(
            100 * coalesce(usuarios_recentes_evadiram, 0)
            / nullif(usuarios_recentes_total, 0)::numeric,
            1
        ) AS usuarios_evasao_perc,
        coalesce(usuarios_recentes_evadiram, 0) AS usuarios_recentes_evadiram,
        usuarios_recentes_total,
        atualizacao_data
    FROM evadiram_vs_total
)

SELECT * FROM final
