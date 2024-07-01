{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
caps_usuarios_ativos_por_estabelecimento_resumo AS (
    SELECT * FROM {{ ref('caps_usuarios_ativos_por_estabelecimento_resumo') }}
),
caps_usuarios_novos_resumo AS (
    SELECT * FROM {{ ref('caps_usuarios_novos_resumo') }}
),
caps_adesao_evasao_coortes_resumo AS (
    SELECT * FROM {{ ref('caps_adesao_evasao_coortes_resumo') }}
),
caps_atendimentos_individuais_por_caps AS (
    SELECT * FROM {{ ref('caps_atendimentos_individuais_por_caps') }}
),
caps_usuarios_atendimentos_individuais_perfil_resumo AS (
    SELECT * FROM {{ ref('caps_usuarios_atendimentos_individuais_perfil_resumo') }}
),
caps_procedimentos_por_usuario_por_estabelecimento AS (
    SELECT * FROM {{ ref('caps_procedimentos_por_usuario_por_estabelecimento') }}
),
caps_procedimentos_por_usuario_por_tempo_servico_resumo AS (
    SELECT * FROM {{ ref('caps_procedimentos_por_usuario_por_tempo_servico_resumo') }}
),
caps_procedimentos_por_hora_resumo AS (
    SELECT * FROM {{ ref('caps_procedimentos_por_hora_resumo') }}
),

usuarios_ativos_resumo AS (
    SELECT
        t.unidade_geografica_id_sus,
        t.tornandose_inativos,
        t.dif_tornandose_inativos_anterior,
        t.ativos_mes,
        t.dif_ativos_mes_anterior,
        t.periodo as periodo_ativos,
        t.nome_mes as nome_mes_ativos
    FROM caps_usuarios_ativos_por_estabelecimento_resumo t
    INNER JOIN (
        SELECT
            unidade_geografica_id_sus,
            MAX(periodo_ordem) AS max_periodo_ordem
        FROM caps_usuarios_ativos_por_estabelecimento_resumo
        WHERE estabelecimento = 'Todos'
            AND estabelecimento_linha_perfil = 'Todos'
            AND estabelecimento_linha_idade = 'Todos'
        GROUP BY unidade_geografica_id_sus
    ) AS p ON t.unidade_geografica_id_sus = p.unidade_geografica_id_sus
        AND t.periodo_ordem = p.max_periodo_ordem
    WHERE t.estabelecimento = 'Todos'
        AND t.estabelecimento_linha_perfil = 'Todos'
        AND t.estabelecimento_linha_idade = 'Todos'
),

usuarios_novos_resumo AS (
    SELECT
        t.unidade_geografica_id_sus,
        t.usuarios_novos,
        t.dif_usuarios_novos_anterior,
        t.periodo as periodo_novos,
        t.nome_mes as nome_mes_novos
    FROM caps_usuarios_novos_resumo t
    INNER JOIN (
        SELECT
            unidade_geografica_id_sus,
            MAX(periodo_ordem) AS max_periodo_ordem
        FROM caps_usuarios_novos_resumo 
        WHERE estabelecimento = 'Todos'
            AND estabelecimento_linha_perfil = 'Todos'
            AND estabelecimento_linha_idade = 'Todos'
        GROUP BY unidade_geografica_id_sus
    ) AS p ON t.unidade_geografica_id_sus = p.unidade_geografica_id_sus
        AND t.periodo_ordem = p.max_periodo_ordem
    WHERE t.estabelecimento = 'Todos' 
        AND t.estabelecimento_linha_perfil = 'Todos' 
        AND t.estabelecimento_linha_idade = 'Todos' 
        
),

nao_adesao_resumo AS (
    SELECT
        t.unidade_geografica_id_sus,
        t.usuarios_coorte_nao_aderiram_perc AS usuarios_coorte_nao_aderiram,
		t.maior_taxa_estabelecimento as maior_taxa_estabelecimento_nao_adesao,
		t.maior_taxa_perc as maior_taxa_nao_adesao,
		t.predominio_sexo,
		t.predominio_faixa_etaria,
		t.predominio_condicao_grupo_descricao_curta_cid10,
		t.a_partir_do_mes,
		t.a_partir_do_ano,
		t.ate_mes,
		t.ate_ano
    FROM caps_adesao_evasao_coortes_resumo t
    INNER JOIN (
        SELECT
            unidade_geografica_id_sus,
            MAX(periodo_ordem) AS max_periodo_ordem
        FROM caps_adesao_evasao_coortes_resumo 
        WHERE estabelecimento = 'Todos'
        GROUP BY unidade_geografica_id_sus
    ) AS p ON t.unidade_geografica_id_sus = p.unidade_geografica_id_sus
        AND t.periodo_ordem = p.max_periodo_ordem
    WHERE t.estabelecimento = 'Todos' 
),

atendimentos_individuais_resumo AS (
    SELECT
        t.unidade_geografica_id_sus,
        t.perc_apenas_atendimentos_individuais,
        t.dif_perc_apenas_atendimentos_individuais,
        t.maior_taxa AS maior_taxa_atendimentos_individuais,
        t.maior_taxa_estabelecimento AS maior_taxa_estabelecimento_atendimentos_individuais,
        t.periodo as periodo_atendimentos_individuais,
        t.nome_mes as nome_mes_atendimentos_individuais
    FROM caps_atendimentos_individuais_por_caps t
    INNER JOIN (
        SELECT
            unidade_geografica_id_sus,
            MAX(periodo_ordem) AS max_periodo_ordem
        FROM caps_atendimentos_individuais_por_caps
        WHERE estabelecimento = 'Todos'
            AND estabelecimento_linha_perfil = 'Todos'
            AND estabelecimento_linha_idade = 'Todos'
        GROUP BY unidade_geografica_id_sus
    ) AS p ON t.unidade_geografica_id_sus = p.unidade_geografica_id_sus
        AND t.periodo_ordem = p.max_periodo_ordem    
    WHERE t.estabelecimento = 'Todos' 
        AND t.estabelecimento_linha_perfil = 'Todos' 
        AND t.estabelecimento_linha_idade = 'Todos' 
),

atendimentos_individuais_perfil_resumo AS (
    SELECT
        unidade_geografica_id_sus,
        sexo_predominante AS sexo_atendimentos_individuais,
        faixa_etaria_predominante AS faixa_etaria_atendimentos_individuais,
        cid_grupo_predominante AS cid_grupo_atendimentos_individuais,
        usuarios_cid_predominante AS usuarios_cid_atendimentos_individuais,
        periodo as periodo_atendimentos_individuais_perfil,
        nome_mes as nome_mes_atendimentos_individuais_perfil
    FROM caps_usuarios_atendimentos_individuais_perfil_resumo
    -- WHERE periodo = 'Último período'
),
procedimentos_por_usuario_estabelecimento_resumo AS (
    SELECT
        t.unidade_geografica_id_sus,
        t.maior_taxa AS maior_taxa_procedimentos_por_usuario,
        t.maior_taxa_estabelecimento AS maior_taxa_estabelecimento_procedimentos_por_usuario,
        t.procedimentos_por_usuario,
        t.dif_procedimentos_por_usuario_anterior_perc,
        t.periodo as periodo_proced_usuario,
        t.nome_mes as nome_mes_proced_usuario
    FROM caps_procedimentos_por_usuario_por_estabelecimento t
    INNER JOIN (
        SELECT
            unidade_geografica_id_sus,
            MAX(periodo_ordem) AS max_periodo_ordem
        FROM caps_procedimentos_por_usuario_por_estabelecimento
        WHERE estabelecimento = 'Todos'
            AND estabelecimento_linha_perfil = 'Todos'
            AND estabelecimento_linha_idade = 'Todos'
        GROUP BY unidade_geografica_id_sus
    ) AS p ON t.unidade_geografica_id_sus = p.unidade_geografica_id_sus
        AND t.periodo_ordem = p.max_periodo_ordem
    WHERE t.estabelecimento = 'Todos' 
        AND t.estabelecimento_linha_perfil = 'Todos' 
        AND t.estabelecimento_linha_idade = 'Todos' 
),

procedimentos_por_usuario_tempo_servico_resumo AS (
    SELECT
        unidade_geografica_id_sus,
        tempo_servico_maior_taxa,
        maior_taxa AS maior_taxa_procedimentos_tempo_servico
    FROM caps_procedimentos_por_usuario_por_tempo_servico_resumo
    WHERE estabelecimento = 'Todos'
),

procedimentos_por_hora_resumo AS (
    SELECT
        t.unidade_geografica_id_sus,
        t.procedimentos_registrados_total,
        t.dif_procedimentos_registrados_total_anterior,
        t.procedimentos_registrados_bpa,
        t.dif_procedimentos_registrados_bpa_anterior,
        t.procedimentos_registrados_raas,
        t.dif_procedimentos_registrados_raas_anterior,
        t.periodo as periodo_procedimentos_hora,
        t.nome_mes as nome_mes_procedimentos_hora
    FROM caps_procedimentos_por_hora_resumo t
    INNER JOIN (
        SELECT
            unidade_geografica_id_sus,
            MAX(periodo_ordem) AS max_periodo_ordem
        FROM caps_procedimentos_por_hora_resumo
        WHERE estabelecimento = 'Todos'
            AND estabelecimento_linha_perfil = 'Todos'
            AND estabelecimento_linha_idade = 'Todos'
            AND ocupacao = 'Todas'
        GROUP BY unidade_geografica_id_sus
    ) AS p ON t.unidade_geografica_id_sus = p.unidade_geografica_id_sus
        AND t.periodo_ordem = p.max_periodo_ordem
    WHERE t.estabelecimento = 'Todos' 
        AND t.estabelecimento_linha_perfil = 'Todos' 
        AND t.estabelecimento_linha_idade = 'Todos' 
        AND periodo = 'Último período' 
        AND t.ocupacao = 'Todas'
),
intermediaria AS (
    SELECT *
    FROM usuarios_ativos_resumo
    FULL JOIN usuarios_novos_resumo
    USING (unidade_geografica_id_sus)
    FULL JOIN nao_adesao_resumo
    USING (unidade_geografica_id_sus)
    left JOIN atendimentos_individuais_resumo
    USING (unidade_geografica_id_sus)
    left JOIN atendimentos_individuais_perfil_resumo
    USING (unidade_geografica_id_sus)
    left JOIN procedimentos_por_usuario_estabelecimento_resumo
    USING (unidade_geografica_id_sus)
    left JOIN procedimentos_por_usuario_tempo_servico_resumo
    USING (unidade_geografica_id_sus)
    left JOIN procedimentos_por_hora_resumo
    USING (unidade_geografica_id_sus)
),
final AS (
    SELECT 
	{{ dbt_utils.surrogate_key([
      		"unidade_geografica_id_sus"
        ]) }} AS id,
	*,
    now() AS atualizacao_data
    FROM intermediaria
)
SELECT 
    * 
FROM final
