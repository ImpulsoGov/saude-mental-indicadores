{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_atendimendos_individuais AS (
    SELECT * FROM {{ ref("_caps_usuarios_atendimentos_individuais") }}
),
perfil_por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia AS periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria_descricao,
        usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        usuario_raca_cor_id_siasus,
        coalesce(
            grupo_descricao_curta_cid10,
            'Outras condições'
        ) AS usuario_condicao_saude,        
        count(DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE fez_procedimentos AND NOT procedimentos_alem_individual
        ) AS usuarios_apenas_atendimento_individual,
        count(DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE fez_procedimentos
        ) AS usuarios_frequentantes
    FROM usuarios_atendimendos_individuais usuario
    WHERE NOT procedimentos_alem_individual
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        estabelecimento_id_scnes,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria_descricao,
        usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        usuario_raca_cor_id_siasus,
        grupo_descricao_curta_cid10
),
{{ classificar_caps_linha(
    relacao="perfil_por_estabelecimento",
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id=none,
    todas_linhas_valor=none,
    cte_resultado="com_linhas_cuidado"
) }}
SELECT * FROM com_linhas_cuidado