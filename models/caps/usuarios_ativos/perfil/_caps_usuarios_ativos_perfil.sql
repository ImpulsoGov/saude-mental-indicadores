{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_ativos AS (
    SELECT *
    FROM {{ ref("caps_usuarios_ativos") }}
	WHERE usuario_primeiro_procedimento_periodo_data_inicio IS NOT NULL
),
condicoes_saude AS (
    SELECT 
        DISTINCT ON (id_cid10)
        *
    FROM {{ source("codigos", "condicoes_saude") }}
    ORDER BY id_cid10
),
por_estabelecimentos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        -- usuario_tempo_servico_id,
        -- usuario_tempo_servico_descricao,
        -- usuario_tempo_servico_ordem,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria_descricao,
        usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        condicoes_saude.grupo_id_cid10,
        condicoes_saude.grupo_descricao_curta_cid10 AS usuario_condicao_saude,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias,
        count (DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE ativo_mes
        ) AS ativos_mes,
        count (DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE ativo_3meses
        ) AS ativos_3meses,
        count (DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE tornandose_inativo
        ) AS tornandose_inativos,
        max(atualizacao_data) AS atualizacao_data
    FROM usuarios_ativos
    LEFT JOIN condicoes_saude
    ON usuarios_ativos.condicao_principal_id_cid10 = condicoes_saude.id_cid10
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        -- usuario_tempo_servico_id,
        -- usuario_tempo_servico_descricao,
        -- usuario_tempo_servico_ordem,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria_descricao,
        usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        condicoes_saude.grupo_id_cid10,
        condicoes_saude.grupo_descricao_curta_cid10,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias
),
{{ classificar_caps_linha(
    relacao="por_estabelecimentos",
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id=none,
    todas_linhas_valor=none,
    cte_resultado="por_estabelecimento_com_linhas_cuidado"
) }}
SELECT * FROM por_estabelecimento_com_linhas_cuidado
