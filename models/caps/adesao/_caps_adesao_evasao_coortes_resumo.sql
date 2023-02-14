{#
SPDX-FileCopyrightText: 2023 ImpulsoGov <contato@impulsogov.org>

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

estabelecimentos AS (
    SELECT 
        DISTINCT ON (estabelecimento_cnes_id)
        unidade_geografica_id,
        municipio_id_sus AS unidade_geografica_id_sus,
        estabelecimento_cnes_id AS estabelecimento_id_scnes
    FROM {{ source("scnes", "estabelecimentos_identificados") }}
    ORDER BY
        estabelecimento_cnes_id,
        estabelecimento_data_atualizacao_base_nacional DESC
),

usuarios_nao_aderiram AS (
    SELECT
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        usuario_primeiro_procedimento_periodo_data_inicio
            AS periodo_data_inicio,
        evadiu,
        atualizacao_data
    FROM {{ ref('_caps_adesao_usuarios_evadiram') }}
),

usuarios_perfil AS (
    SELECT
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        usuario_residencia_municipio_id_sus,
        usuario_nascimento_data,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        atualizacao_data
    FROM {{ ref("_raps_usuarios_atendidos") }}
),

usuarios_nao_aderiram_perfil AS (
    SELECT
        usuarios_perfil.periodo_id,
        usuarios_nao_aderiram.periodo_data_inicio,
        usuarios_nao_aderiram.estabelecimento_id_scnes,
        usuarios_nao_aderiram.usuario_id_cns_criptografado,
        usuarios_perfil.usuario_nascimento_data,
        usuarios_perfil.usuario_sexo_id_sigtap,
        condicoes_saude.grupo_descricao_curta_cid10,
        usuarios_nao_aderiram.evadiu,
        greatest(
            usuarios_nao_aderiram.atualizacao_data,
            usuarios_perfil.atualizacao_data
        ) AS atualizacao_data
    FROM usuarios_nao_aderiram
    LEFT JOIN usuarios_perfil
        ON usuarios_nao_aderiram.estabelecimento_id_scnes
            = usuarios_perfil.estabelecimento_id_scnes
        AND usuarios_nao_aderiram.periodo_data_inicio
            = usuarios_perfil.periodo_data_inicio
        AND usuarios_nao_aderiram.usuario_id_cns_criptografado
            = usuarios_perfil.usuario_id_cns_criptografado
    LEFT JOIN condicoes_saude
        ON usuarios_perfil.condicao_principal_id_cid10
            = condicoes_saude.id_cid10
),

{{ classificar_faixa_etaria(
    relacao="usuarios_nao_aderiram_perfil",
    coluna_nascimento_data="usuario_nascimento_data",
    coluna_data_referencia="periodo_data_inicio",
    idade_tipo="Anos",
    faixa_etaria_agrupamento="10 em 10 anos",
    colunas_faixa_etaria=["descricao"],
    cte_resultado="com_faixa_etaria"
) }},

com_unidades_geograficas AS (
    SELECT
        estabelecimentos.unidade_geografica_id,
        estabelecimentos.unidade_geografica_id_sus,
        com_faixa_etaria.*
    FROM com_faixa_etaria
    LEFT JOIN estabelecimentos
    ON com_faixa_etaria.estabelecimento_id_scnes
        = estabelecimentos.estabelecimento_id_scnes
),

resumo AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        coalesce(
            estabelecimento_id_scnes,
            '0000000'
        ) AS estabelecimento_id_scnes,
        count(DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE evadiu
        ) AS usuarios_coorte_nao_aderiram,
        count(DISTINCT usuario_id_cns_criptografado) AS usuarios_coorte_total,
        mode() WITHIN GROUP (
            ORDER BY usuario_sexo_id_sigtap
        ) FILTER (WHERE evadiu) AS predominio_sexo_id_sigtap,
        mode() WITHIN GROUP (
            ORDER BY faixa_etaria_descricao
        ) FILTER (WHERE evadiu) AS predominio_faixa_etaria,
        mode() WITHIN GROUP (
            ORDER BY grupo_descricao_curta_cid10
        ) FILTER (
            WHERE evadiu
        ) AS predominio_condicao_grupo_descricao_curta_cid10,
        max(atualizacao_data) AS atualizacao_data,
        count(*) AS controle
    FROM com_unidades_geograficas
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        ROLLUP(estabelecimento_id_scnes)
),

com_percentuais AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
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
        predominio_sexo_id_sigtap,
        predominio_faixa_etaria,
        predominio_condicao_grupo_descricao_curta_cid10,
        atualizacao_data
    FROM resumo
),

caps_pior_taxa AS (
    SELECT 
        DISTINCT ON (
            unidade_geografica_id_sus,
            unidade_geografica_id,
            periodo_data_inicio,
            periodo_id
        )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes AS maior_taxa_estabelecimento_id_scnes,
        usuarios_coorte_nao_aderiram_perc AS maior_taxa_perc,
        usuarios_coorte_nao_aderiram AS maior_taxa_usuarios_nao_aderiram,
        usuarios_coorte_total AS maior_taxa_usuarios_total,
        atualizacao_data
    FROM com_percentuais
    ORDER BY
        unidade_geografica_id_sus,
        unidade_geografica_id,
        periodo_data_inicio,
        periodo_id,
        usuarios_coorte_nao_aderiram_perc DESC,
        usuarios_coorte_nao_aderiram DESC
),

com_pior_taxa AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        usuarios_coorte_nao_aderiram_perc,
        usuarios_coorte_nao_aderiram,
        usuarios_coorte_total,
        predominio_sexo_id_sigtap,
        predominio_faixa_etaria,
        predominio_condicao_grupo_descricao_curta_cid10,
        maior_taxa_estabelecimento_id_scnes,
        maior_taxa_perc,
        maior_taxa_usuarios_nao_aderiram,
        maior_taxa_usuarios_total,
        greatest(
            com_percentuais.atualizacao_data,
            caps_pior_taxa.atualizacao_data
        ) AS atualizacao_data
    FROM com_percentuais
    LEFT JOIN caps_pior_taxa
    USING (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_data_inicio,
        periodo_id
    )
),

-- Exclui as 4 competências mais recentes, já que nelas ainda não houve tempo 
-- para observar o comportamento (adesão/evasão) nos três meses iniciais após
-- o primeiro procedimento, mais os 2 meses necessários para confirmar as
-- evasões ocorridas nas últimas competências dentro desse período
{{ ultimas_competencias(
    relacao="com_pior_taxa",
    fontes=["bpa_i_disseminacao", "raas_psicossocial_disseminacao"],
    meses_antes_ultima_competencia=(4, none),
    cte_resultado="exceto_4_ultimas"
) }},

final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "estabelecimento_id_scnes",
            "periodo_id"
        ]) }} AS id,
        *
    FROM exceto_4_ultimas
)

SELECT * FROM final
