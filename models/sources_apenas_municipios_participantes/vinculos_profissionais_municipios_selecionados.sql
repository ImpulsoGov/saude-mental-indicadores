
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
vinculos_profissionais AS (
	SELECT 
        estabelecimento_id_scnes,
        estabelecimento_municipio_id_sus,
        -- estabelecimento_regiao_saude_id_sus,
        -- estabelecimento_microrregiao_saude_id_sus,
        -- estabelecimento_distrito_sanitario_id_sus,
        -- estabelecimento_distrito_administrativo_id_sus,
        -- estabelecimento_gestao_condicao_id_scnes,
        -- estabelecimento_personalidade_juridica_id_scnes,
        -- estabelecimento_id_cpf_cnpj,
        -- estabelecimento_mantido,
        -- estabelecimento_mantenedora_id_cnpj,
        -- estabelecimento_esfera_id_scnes,
        -- estabelecimento_atividade_ensino_id_scnes,
        -- estabelecimento_tributos_retencao_id_scnes,
        estabelecimento_natureza_id_scnes,
        -- estabelecimento_fluxo_id_scnes,
        -- estabelecimento_tipo_id_scnes,
        -- estabelecimento_turno_id_scnes,
        -- estabelecimento_hierarquia_id_scnes,
        -- estabelecimento_terceiro,
        profissional_id_cpf_criptografado,
        profissional_cpf_unico,
        ocupacao_id_cbo2002,
        ocupacao_cbo_unico,
        profissional_nome,
        profissional_id_cns,
        -- profissional_conselho_tipo_id_scnes,
        -- profissional_id_conselho,
        tipo_id_scnes,
        contratado,
        autonomo,
        sem_vinculo_definido,
        atendimento_sus,
        atendimento_nao_sus,
        atendimento_carga_outras,
        atendimento_carga_hospitalar,
        atendimento_carga_ambulatorial,
        periodo_data_inicio,
        -- profissional_residencia_municipio_id_sus,
        estabelecimento_natureza_juridica_id_scnes,
        id,
        periodo_id,
        unidade_geografica_id,
        criacao_data,
        atualizacao_data
    FROM {{ source("scnes", "vinculos_profissionais") }}
), 

{{ selecionar_municipios_ativos(
	relacao="vinculos_profissionais",
    coluna_municipio_id="unidade_geografica_id",
	cte_resultado="municipios_selecionados"
) }},

{{ remover_estabelecimentos_indesejados(
	relacao="municipios_selecionados",
	cte_resultado="municipios_selecionados_filtrados"
) }},

{{ limitar_quantidade_meses(
	relacao="municipios_selecionados_filtrados",
    coluna_data="periodo_data_inicio",
    coluna_municipio_id="unidade_geografica_id",
	cte_resultado="final"
) }}

SELECT * FROM final
