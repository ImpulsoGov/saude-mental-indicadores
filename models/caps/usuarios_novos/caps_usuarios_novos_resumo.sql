{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
usuarios_novos AS (
    SELECT * FROM {{ ref('caps_usuarios_novos_perfil') }}
),
usuarios_ativos AS (
    SELECT * FROM {{ ref('caps_usuarios_ativos_por_estabelecimento_resumo') }}
),
resumo_usuarios_novos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo,
        periodo_ordem,
        nome_mes,
        competencia,
        estabelecimento_linha_perfil,
        estabelecimento_linha_idade,
        estabelecimento,
        sum(usuarios_novos) AS usuarios_novos,
        sum(usuarios_novos_anterior) AS usuarios_novos_anterior,
        sum(dif_usuarios_novos_anterior) AS dif_usuarios_novos_anterior
    FROM usuarios_novos
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo,
        periodo_ordem,
        nome_mes,
        competencia,
        estabelecimento_linha_perfil,
        estabelecimento_linha_idade,
        estabelecimento
),
final AS (
    SELECT 
        usuarios_ativos.id,
        usuarios_ativos.unidade_geografica_id,
        usuarios_ativos.unidade_geografica_id_sus,
        usuarios_ativos.periodo_id,        
        usuarios_ativos.competencia,
        usuarios_ativos.estabelecimento_linha_perfil,
        usuarios_ativos.estabelecimento_linha_idade,
        coalesce(usuarios_novos, 0) AS usuarios_novos,
        coalesce(usuarios_novos_anterior, 0) AS usuarios_novos_anterior,
        coalesce(dif_usuarios_novos_anterior, 0) AS dif_usuarios_novos_anterior,
        usuarios_ativos.estabelecimento,
        usuarios_ativos.periodo,
        usuarios_ativos.nome_mes,
        usuarios_ativos.periodo_ordem
    FROM resumo_usuarios_novos
    -- Junta com usuários ativos para garantir que todos os CAPS com usuários ativos
    -- apareçam na consulta final, mesmo que não haja usuários novos no mês nem no
    -- anterior
    RIGHT JOIN
        usuarios_ativos
    USING (
        unidade_geografica_id,
        periodo_id,
        estabelecimento,
        estabelecimento_linha_perfil,
        estabelecimento_linha_idade
    )
)
SELECT * FROM final