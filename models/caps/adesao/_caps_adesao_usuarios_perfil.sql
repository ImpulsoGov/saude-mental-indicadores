{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
condicoes_saude AS (
    SELECT 
        DISTINCT ON (id_cid10)
        *
    FROM {{ source("codigos", "condicoes_saude") }}
    ORDER BY id_cid10
),

usuarios_recentes AS (
    SELECT
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria_descricao,
        usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        usuario_primeiro_procedimento_periodo_data_inicio
            AS primeiro_procedimento_periodo_data_inicio,
        atualizacao_data
    FROM {{ ref("caps_usuarios_ativos") }}
    WHERE usuario_primeiro_procedimento_periodo_data_inicio IS NOT NULL
        AND (
            periodo_data_inicio
            >= usuario_primeiro_procedimento_periodo_data_inicio
        )
        AND periodo_data_inicio < (
            usuario_primeiro_procedimento_periodo_data_inicio
            + '3 mon'::interval
        )
),

usuarios_estatus_adesao AS (
    SELECT
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        evadiu_a_partir_de_periodo_data_inicio,
        evadiu,
        atualizacao_data
    FROM {{ ref('_caps_adesao_usuarios_evadiram') }}
),

usuarios_perfil AS (
    SELECT
        usuarios_recentes.periodo_id,
        usuarios_recentes.periodo_data_inicio,
        usuarios_recentes.unidade_geografica_id,
        usuarios_recentes.unidade_geografica_id_sus,
        usuarios_recentes.estabelecimento_id_scnes,
        usuarios_recentes.usuario_id_cns_criptografado,
        usuarios_recentes.usuario_faixa_etaria_id,
        usuarios_recentes.usuario_faixa_etaria_descricao,
        usuarios_recentes.usuario_faixa_etaria_ordem,
        usuarios_recentes.usuario_sexo_id_sigtap,
        usuarios_recentes.usuario_raca_cor_id_siasus,
        condicoes_saude.grupo_descricao_curta_cid10,
        usuarios_recentes.primeiro_procedimento_periodo_data_inicio,
        usuarios_estatus_adesao.evadiu_a_partir_de_periodo_data_inicio,
        usuarios_estatus_adesao.evadiu,
        greatest(
            usuarios_recentes.atualizacao_data,
            usuarios_estatus_adesao.atualizacao_data
        ) AS atualizacao_data
    FROM usuarios_recentes
    LEFT JOIN usuarios_estatus_adesao
        ON usuarios_estatus_adesao.estabelecimento_id_scnes
            = usuarios_recentes.estabelecimento_id_scnes
        AND usuarios_estatus_adesao.usuario_id_cns_criptografado
            = usuarios_recentes.usuario_id_cns_criptografado
    LEFT JOIN condicoes_saude
        ON usuarios_recentes.condicao_principal_id_cid10 = condicoes_saude.id_cid10
    WHERE
        (
            usuarios_estatus_adesao.evadiu_a_partir_de_periodo_data_inicio IS NULL
            OR (
                periodo_data_inicio
                <= usuarios_estatus_adesao.evadiu_a_partir_de_periodo_data_inicio
            )
        )
        -- Remove usuários desconsiderados na conta (perfil ambulatorial e 
        -- encaminhamentos para ambulatório)
        AND usuarios_estatus_adesao.evadiu IS NOT NULL
),


-- Exclui as 4 competências mais recentes, já que nelas ainda não houve tempo 
-- para observar o comportamento (adesão/evasão) nos três meses iniciais após
-- o primeiro procedimento, mais os 2 meses necessários para confirmar as
-- evasões ocorridas nas últimas competências dentro desse período
{{ ultimas_competencias(
    relacao="usuarios_perfil",
    fontes=["bpa_i_disseminacao", "raas_psicossocial_disseminacao"],
    meses_antes_ultima_competencia=(4, none),
    cte_resultado="exceto_ultimas_4_competencias"
) }},

resumo AS (
    SELECT
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria_descricao,
        usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        grupo_descricao_curta_cid10,
        (
            CASE
                WHEN (
                    periodo_data_inicio 
                    = evadiu_a_partir_de_periodo_data_inicio
                ) THEN 'Evadiram no mês'
                ELSE 'Em seguimento'
            END
        ) AS estatus_adesao_mes,
        (
            CASE
                WHEN NOT evadiu THEN 'Aderiram'
                WHEN evadiu THEN 'Não aderiram'
                ELSE NULL
            END
        ) AS estatus_adesao_final,
        count(*) AS quantidade_registrada,
        max(atualizacao_data) AS atualizacao_data
    FROM exceto_ultimas_4_competencias
    GROUP BY
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria_descricao,
        usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        grupo_descricao_curta_cid10,
        evadiu,
        (periodo_data_inicio = evadiu_a_partir_de_periodo_data_inicio)
),

final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "periodo_id",
            "unidade_geografica_id",
            "estabelecimento_id_scnes",
            "usuario_faixa_etaria_id",
            "usuario_sexo_id_sigtap",
            "grupo_descricao_curta_cid10",
            "estatus_adesao_mes",
            "estatus_adesao_final"
        ])}} AS id,
        resumo.*
    FROM resumo
)
SELECT * FROM final
