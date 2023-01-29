{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
periodos AS (
    SELECT * FROM {{ source("codigos", "periodos") }}
),

estabelecimentos AS (
    SELECT 
        DISTINCT ON (estabelecimento_cnes_id)
        *
    FROM {{ source("scnes", "estabelecimentos_identificados") }}
    ORDER BY
        estabelecimento_cnes_id,
        estabelecimento_data_atualizacao_base_nacional DESC
),

usuarios_estatus_evasao AS (
    SELECT * FROM {{ ref ("_caps_adesao_usuarios_evadiram") }}
),

estatus_por_coorte_inicio AS (
    SELECT
        (
            usuario_primeiro_procedimento_periodo_data_inicio
        ) AS periodo_data_inicio,
        estabelecimento_id_scnes,
        count(DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE evadiu
        ) AS usuarios_coorte_nao_aderiram,
        count(DISTINCT usuario_id_cns_criptografado) AS usuarios_coorte_total,
        max(atualizacao_data) AS atualizacao_data
    FROM usuarios_estatus_evasao
    WHERE evadiu IS NOT NULL
    GROUP BY
        periodo_data_inicio,
        estabelecimento_id_scnes
),

final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "estabelecimento_id_scnes",
            "periodos.id"
        ]) }} AS id,
        estabelecimentos.unidade_geografica_id AS unidade_geografica_id,
        estabelecimentos.municipio_id_sus AS unidade_geografica_id_sus,
        periodos.id AS periodo_id,
        periodo_data_inicio,
        (
            periodo_data_inicio + '2 mon'::interval
        ) AS coorte_fim_periodo_data_inicio,
        estabelecimento_id_scnes,
        round(
            100 * coalesce(usuarios_coorte_nao_aderiram, 0)
            / nullif(usuarios_coorte_total, 0)::numeric,
            1
        ) AS usuarios_coorte_nao_aderiram_perc,
        usuarios_coorte_nao_aderiram,
        usuarios_coorte_total,
        estatus_por_coorte_inicio.atualizacao_data AS atualizacao_data
    FROM estatus_por_coorte_inicio
    -- TODO: trocar com vinculação com tabela de referencia de estabelecimentos
    -- para obter de lá a unidade geográfica
    LEFT JOIN estabelecimentos
    ON estatus_por_coorte_inicio.estabelecimento_id_scnes
        = estabelecimentos.estabelecimento_cnes_id
    LEFT JOIN periodos
    ON estatus_por_coorte_inicio.periodo_data_inicio = periodos.data_inicio
    WHERE periodos.tipo = 'Mensal'
)

SELECT * FROM final
