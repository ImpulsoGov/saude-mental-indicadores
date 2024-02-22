
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
bpa_i_disseminacao AS (
	SELECT 
        id,
        estabelecimento_id_scnes,
        -- gestao_unidade_geografica_id_sus,
        -- gestao_condicao_id_siasus,
        unidade_geografica_id_sus,
        estabelecimento_tipo_id_sigtap,
        -- prestador_tipo_id_sigtap,
        -- estabelecimento_mantido,
        -- estabelecimento_id_cnpj,
        -- mantenedora_id_cnpj,
        processamento_periodo_data_inicio,
        realizacao_periodo_data_inicio,
        procedimento_id_sigtap,
        -- financiamento_tipo_id_sigtap,
        -- financiamento_subtipo_id_sigtap,
        -- complexidade_id_siasus,
        -- autorizacao_id_siasus,
        profissional_id_cns,
        profissional_vinculo_ocupacao_id_cbo2002,
        condicao_principal_id_cid10,
        carater_atendimento_id_siasus,
        usuario_id_cns_criptografado,
        usuario_nascimento_data,
        usuario_idade_tipo_id_sigtap,
        usuario_idade,
        usuario_sexo_id_sigtap,
        usuario_raca_cor_id_siasus,
        usuario_residencia_municipio_id_sus,
        quantidade_apresentada,
        quantidade_aprovada,
        valor_apresentado,
        valor_aprovado,
        -- atendimento_residencia_ufs_distintas,
        -- atendimento_residencia_municipios_distintos,
        -- receptor_credito_id_cnpj,
        usuario_etnia_id_sus,
        -- estabelecimento_natureza_juridica_id_scnes,
        unidade_geografica_id,
        periodo_id,
        criacao_data,
        atualizacao_data 
    FROM {{ source("siasus", "bpa_i_disseminacao") }}
), 

{{ selecionar_municipios_ativos(
	relacao="bpa_i_disseminacao",
	cte_resultado="municipios_selecionados"
) }},

{{ remover_estabelecimentos_indesejados(
	relacao="municipios_selecionados",
	cte_resultado="municipios_selecionados_filtrados"
) }},

{{ limitar_quantidade_meses(
	relacao="municipios_selecionados_filtrados",
	cte_resultado="final"
) }}

SELECT * FROM final
