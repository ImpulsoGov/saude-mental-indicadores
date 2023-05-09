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
            unidade_geografica_id_sus,
            tornandose_inativos,
            dif_tornandose_inativos_anterior,
            ativos_mes,
            dif_ativos_mes_anterior,
            periodo as periodo_ativos,
            nome_mes as nome_mes_ativos
        FROM saude_mental.caps_usuarios_ativos_por_estabelecimento_resumo
        WHERE estabelecimento = 'Todos'
            AND estabelecimento_linha_perfil = 'Todos'
            AND estabelecimento_linha_idade = 'Todos'
            AND periodo = 'Último período'
),
usuarios_novos_resumo AS (
    SELECT
        unidade_geografica_id_sus,
        usuarios_novos,
        dif_usuarios_novos_anterior,
        periodo as periodo_novos,
        nome_mes as nome_mes_novos
    FROM saude_mental.caps_usuarios_novos_resumo
    WHERE estabelecimento = 'Todos' AND estabelecimento_linha_perfil = 'Todos' AND estabelecimento_linha_idade = 'Todos' AND periodo = 'Último período'
),
nao_adesao_resumo AS (
    SELECT
        unidade_geografica_id_sus,
        usuarios_coorte_nao_aderiram_perc AS usuarios_coorte_nao_aderiram,
		maior_taxa_estabelecimento as maior_taxa_estabelecimento_nao_adesao,
		maior_taxa_perc as maior_taxa_nao_adesao,
		predominio_sexo,
		predominio_faixa_etaria,
		predominio_condicao_grupo_descricao_curta_cid10,
		a_partir_do_mes,
		a_partir_do_ano,
		ate_mes,
		ate_ano
    FROM saude_mental.caps_adesao_evasao_coortes_resumo
    WHERE estabelecimento = 'Todos' AND periodo = 'Último período'
),
atendimentos_individuais_resumo AS (
    SELECT
        unidade_geografica_id_sus,
        perc_apenas_atendimentos_individuais,
        dif_perc_apenas_atendimentos_individuais,
        maior_taxa AS maior_taxa_atendimentos_individuais,
        maior_taxa_estabelecimento AS maior_taxa_estabelecimento_atendimentos_individuais,
        periodo as periodo_atendimentos_individuais,
        nome_mes as nome_mes_atendimentos_individuais
    FROM saude_mental.caps_atendimentos_individuais_por_caps
    WHERE estabelecimento = 'Todos' AND estabelecimento_linha_perfil = 'Todos' AND estabelecimento_linha_idade = 'Todos' AND periodo = 'Último período'
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
    FROM saude_mental.caps_usuarios_atendimentos_individuais_perfil_resumo
    -- WHERE periodo = 'Último período'
),
procedimentos_por_usuario_estabelecimento_resumo AS (
    SELECT
        unidade_geografica_id_sus,
        maior_taxa AS maior_taxa_procedimentos_por_usuario,
        maior_taxa_estabelecimento AS maior_taxa_estabelecimento_procedimentos_por_usuario,
        procedimentos_por_usuario,
        dif_procedimentos_por_usuario_anterior_perc,
        periodo as periodo_proced_usuario,
        nome_mes as nome_mes_proced_usuario
    FROM saude_mental.caps_procedimentos_por_usuario_por_estabelecimento
    WHERE estabelecimento = 'Todos' AND estabelecimento_linha_perfil = 'Todos' AND estabelecimento_linha_idade = 'Todos' AND periodo = 'Último período'
),
procedimentos_por_usuario_tempo_servico_resumo AS (
    SELECT
        unidade_geografica_id_sus,
        tempo_servico_maior_taxa,
        maior_taxa AS maior_taxa_procedimentos_tempo_servico
    FROM saude_mental.caps_procedimentos_por_usuario_por_tempo_servico_resumo
    WHERE estabelecimento = 'Todos'
),
procedimentos_por_hora_resumo AS (
    SELECT
        unidade_geografica_id_sus,
        procedimentos_registrados_total,
        dif_procedimentos_registrados_total_anterior,
        procedimentos_registrados_bpa,
        dif_procedimentos_registrados_bpa_anterior,
        procedimentos_registrados_raas,
        dif_procedimentos_registrados_raas_anterior,
        periodo as periodo_procedimentos_hora,
        nome_mes as nome_mes_procedimentos_hora
    FROM saude_mental.caps_procedimentos_por_hora_resumo
    WHERE estabelecimento = 'Todos' AND estabelecimento_linha_perfil = 'Todos' AND estabelecimento_linha_idade = 'Todos' AND periodo = 'Último período' AND ocupacao = 'Todas'
),
final AS (
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
)
SELECT * FROM final