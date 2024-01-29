
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
aih_rd_disseminacao AS (
	SELECT 
        id,
        unidade_geografica_id,
        periodo_id,
        gestao_unidade_geografica_id_sus,
        leito_especialidade_id_sigtap,
        estabelecimento_id_cnpj,
        aih_id_sihsus,
        usuario_residencia_cep,
        usuario_residencia_municipio_id_sus,
        usuario_nascimento_data,
        usuario_sexo_id_sihsus,
        uti_diarias,
        uti_tipo_id_sihsus,
        unidade_intermediaria_diarias,
        acompanhante_diarias,
        diarias,
        procedimento_solicitado_id_sigtap,
        procedimento_realizado_id_sigtap,
        -- valor_servicos_hospitalares
        -- valor_servicos_profissionais
        -- valor_total
        -- valor_uti
        -- valor_total_dolar
        aih_data_inicio,
        aih_data_fim,
        condicao_principal_id_cid10,
        aih_tipo_id_sihsus,
        condicao_secundaria_id_cid10,
        desfecho_motivo_id_sihsus,
        -- estabelecimento_natureza_id_scnes
        -- estabelecimento_natureza_juridica_id_scnes
        gestao_condicao_id_sihsus,
        -- exame_vdrl
        unidade_geografica_id_sus,
        usuario_idade_tipo_id_sigtap,
        usuario_idade,
        permanencia_duracao,
        obito,
        usuario_nacionalidade_id_sigtap,
        carater_atendimento_id_sihsus,
        -- usuario_homonimo
        -- usuario_filhos_quantidade
        usuario_instrucao_id_sihsus,
        condicao_notificacao_id_cid10,
        -- usuario_contraceptivo_principal_id_sihsus
        -- usuario_contraceptivo_secundario_id_sihsus
        -- gestacao_risco
        -- usuario_id_pre_natal
        remessa_aih_id_sequencial_longa_permanencia,
        usuario_ocupacao_id_cbo2002,
        usuario_atividade_id_cnae,
        usuario_vinculo_previdencia_id_sihsus,
        -- autorizacao_gestor_motivo_id_sihsus
        -- autorizacao_gestor_tipo_id_sihsus
        -- autorizacao_gestor_id_cpf
        -- autorizacao_gestor_data
        estabelecimento_id_scnes,
        mantenedora_id_cnpj,
        -- infeccao_hospitalar
        condicao_associada_id_cid10,
        condicao_obito_id_cid10,
        complexidade_id_sihsus,
        -- financiamento_tipo_id_sigtap
        -- financiamento_subtipo_id_sigtap
        -- regra_contratual_id_scnes
        usuario_raca_cor_id_sihsus,
        usuario_etnia_id_sus,
        remessa_aih_id_sequencial,
        remessa_id_sihsus,
        cns_ausente_justificativa_auditor,
        cns_ausente_justificativa_estabelecimento,
        -- valor_servicos_hospitalares_complemento_federal
        -- valor_servicos_profissionais_complemento_federal
        -- valor_servicos_hospitalares_complemento_local
        -- valor_servicos_profissionais_complemento_local
        -- valor_unidade_neonatal
        -- unidade_neonatal_tipo_id_sihsus
        condicao_secundaria_1_id_cid10,
        condicao_secundaria_2_id_cid10,
        condicao_secundaria_3_id_cid10,
        condicao_secundaria_4_id_cid10,
        condicao_secundaria_5_id_cid10,
        condicao_secundaria_6_id_cid10,
        condicao_secundaria_7_id_cid10,
        condicao_secundaria_8_id_cid10,
        condicao_secundaria_9_id_cid10,
        condicao_secundaria_1_tipo_id_sihsus,
        condicao_secundaria_2_tipo_id_sihsus,
        condicao_secundaria_3_tipo_id_sihsus,
        condicao_secundaria_4_tipo_id_sihsus,
        condicao_secundaria_5_tipo_id_sihsus,
        condicao_secundaria_6_tipo_id_sihsus,
        condicao_secundaria_7_tipo_id_sihsus,
        condicao_secundaria_8_tipo_id_sihsus,
        condicao_secundaria_9_tipo_id_sihsus,
        periodo_data_inicio,
        criacao_data,
        atualizacao_data
        -- _nao_documentado_uti_mes_in
        -- _nao_documentado_uti_mes_an
        -- _nao_documentado_uti_mes_al
        -- _nao_documentado_uti_int_in
        -- _nao_documentado_uti_int_an
        -- _nao_documentado_uti_int_al
        -- _nao_documentado_val_sadt
        -- _nao_documentado_val_rn
        -- _nao_documentado_val_acomp
        -- _nao_documentado_val_ortp
        -- _nao_documentado_val_sangue
        -- _nao_documentado_val_sadtsr
        -- _nao_documentado_val_transp
        -- _nao_documentado_val_obsang
        -- _nao_documentado_val_ped1ac
        -- _nao_documentado_rubrica
        -- _nao_documentado_num_proc
        -- _nao_documentado_tot_pt_sp
        -- _nao_documentado_cpf_aut
    FROM {{ source("sihsus", "aih_rd_disseminacao") }}    
),

{{ selecionar_municipios_ativos(
	relacao="aih_rd_disseminacao",
	cte_resultado="municipios_selecionados"
) }},

{{ limitar_quantidade_meses(
	relacao="municipios_selecionados",
    coluna_data="periodo_data_inicio",
	cte_resultado="final"
) }}

SELECT * FROM final
