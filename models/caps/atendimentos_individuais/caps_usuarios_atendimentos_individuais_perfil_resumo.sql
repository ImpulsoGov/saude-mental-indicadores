{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
raas_ultima_competencia AS (
    SELECT * FROM {{ ref("raas_psicossocial_disseminacao_ultima_competencia") }}
),
bpa_ultima_competencia AS (
    SELECT * FROM {{ ref("bpa_i_disseminacao_ultima_competencia") }}
),
usuarios_atendimentos_individuais_perfil AS (
    SELECT * FROM {{ ref("caps_usuarios_atendimentos_individuais_perfil") }}
),
ultima_competencia_disponivel AS (
    SELECT
        raas_ultima_competencia.unidade_geografica_id,
        least(
            raas_ultima_competencia.periodo_data_inicio,
            bpa_ultima_competencia.periodo_data_inicio
        ) AS periodo_data_inicio
    FROM raas_ultima_competencia
    LEFT JOIN bpa_ultima_competencia
    USING (
        unidade_geografica_id
    )
),
perfil_ultimo_mes AS (
    SELECT
        usuarios_atendimentos_individuais_perfil.*
    FROM usuarios_atendimentos_individuais_perfil
    INNER JOIN ultima_competencia_disponivel
    ON 
        usuarios_atendimentos_individuais_perfil.unidade_geografica_id
        = ultima_competencia_disponivel.unidade_geografica_id
    AND usuarios_atendimentos_individuais_perfil.competencia
        = ultima_competencia_disponivel.periodo_data_inicio
),
perfil_sexo AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        periodo,
        nome_mes, 
        periodo_ordem,
        usuario_sexo,
        sum(usuarios_apenas_atendimento_individual) FILTER (
            WHERE 
                estabelecimento_linha_perfil = 'Todos' AND
                estabelecimento_linha_idade = 'Todos' AND
                estabelecimento != 'Todos'
        ) AS usuarios_apenas_atendimento_individual
--        round(
--            100 * sum(usuarios_apenas_atendimento_individual)::NUMERIC
--            / nullif(sum(usuarios_frequentantes), 0),
--            2
--        ) AS perc_apenas_atendimentos_individuais
    FROM perfil_ultimo_mes
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        periodo,
        nome_mes, 
        periodo_ordem,
        usuario_sexo
),
sexo_predominante AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        periodo,
        nome_mes, 
        periodo_ordem,
        usuario_sexo AS sexo_predominante,
        (
            usuarios_apenas_atendimento_individual
        ) AS usuarios_sexo_predominante
    FROM perfil_sexo
    ORDER BY
        unidade_geografica_id,
        usuarios_apenas_atendimento_individual DESC        
),
perfil_faixa_etaria AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_faixa_etaria_descricao,
        sum(usuarios_apenas_atendimento_individual) FILTER (
            WHERE 
                estabelecimento_linha_perfil = 'Todos' AND
                estabelecimento_linha_idade = 'Todos' AND
                estabelecimento != 'Todos'
        ) AS usuarios_apenas_atendimento_individual
--        round(
--            100 * sum(usuarios_apenas_atendimento_individual)::NUMERIC
--            / nullif(sum(usuarios_frequentantes), 0),
--            2
--        ) AS perc_apenas_atendimentos_individuais
    FROM perfil_ultimo_mes
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_faixa_etaria_descricao
),
faixa_etaria_predominante AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_faixa_etaria_descricao AS faixa_etaria_predominante,
        (
            usuarios_apenas_atendimento_individual
        ) AS usuarios_faixa_etaria_predominante
    FROM perfil_faixa_etaria
    ORDER BY
        unidade_geografica_id,
        usuarios_apenas_atendimento_individual DESC        
),
perfil_cid_grupo AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_condicao_saude,
        sum(usuarios_apenas_atendimento_individual) FILTER (
            WHERE 
                estabelecimento_linha_perfil = 'Todos' AND
                estabelecimento_linha_idade = 'Todos' AND
                estabelecimento != 'Todos'
        ) AS usuarios_apenas_atendimento_individual
--        round(
--            100 * sum(usuarios_apenas_atendimento_individual)::NUMERIC
--            / nullif(sum(usuarios_frequentantes), 0),
--            2
--        ) AS perc_apenas_atendimentos_individuais
    FROM perfil_ultimo_mes
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_condicao_saude
),
cid_grupo_predominante AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_condicao_saude AS cid_grupo_predominante,
        usuarios_apenas_atendimento_individual AS usuarios_cid_predominante
    FROM perfil_cid_grupo
    ORDER BY
        unidade_geografica_id,
        usuarios_apenas_atendimento_individual DESC        
),
intermediaria AS (    
    SELECT 
        *
    FROM sexo_predominante
    FULL JOIN faixa_etaria_predominante
    USING (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id
    )
    FULL JOIN cid_grupo_predominante
    USING (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id
    )
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id"
        ]) }} AS id,
        *,
        now() AS atualizacao_data      
    FROM intermediaria
)
SELECT * FROM final