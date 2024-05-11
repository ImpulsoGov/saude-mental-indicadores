
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
estabelecimentos_habilitacoes AS (
	SELECT 
        estabelecimento_id_scnes,
        estabelecimento_municipio_id_sus AS unidade_geografica_id_sus,
        -- estabelecimento_regiao_saude_id_sus,
        /* estabelecimento_microrregiao_saude_id_sus,
        estabelecimento_distrito_sanitario_id_sus,
        estabelecimento_distrito_administrativo_id_sus,
        estabelecimento_gestao_condicao_id_scnes,
        estabelecimento_personalidade_juridica_id_scnes,
        estabelecimento_id_cpf_cnpj,
        estabelecimento_mantido,
        estabelecimento_mantenedora_id_cnpj,
        estabelecimento_esfera_id_scnes,
        estabelecimento_tributos_retencao_id_scnes,
        estabelecimento_atividade_ensino_id_scnes, */
        estabelecimento_natureza_id_scnes,
        -- estabelecimento_fluxo_id_scnes,
        estabelecimento_tipo_id_scnes,
        estabelecimento_turno_id_scnes,
        estabelecimento_hierarquia_id_scnes,
        -- estabelecimento_terceiro,
        -- estabelecimento_cep,
        atendimento_sus,
        -- prestador_tipo_id_fca,
        habilitacao_id_scnes,
        vigencia_data_inicio,
        vigencia_data_fim,
        -- portaria_data,
        -- portaria_nome,
        -- portaria_periodo_data_inicio,
        -- leitos_quantidade,
        CAST(periodo_data_inicio AS DATE) AS periodo_data_inicio,
        estabelecimento_natureza_juridica_id_scnes,
        criacao_data,
        atualizacao_data,
        id,
        CAST(periodo_id AS UUID) AS periodo_id,
        CAST(unidade_geografica_id AS UUID) AS unidade_geografica_id
    FROM {{ source("scnes", "estabelecimentos_habilitacoes") }}    
),

{{ selecionar_municipios_ativos(
	relacao="estabelecimentos_habilitacoes",
	cte_resultado="municipios_selecionados"
) }},

{{ remover_estabelecimentos_indesejados(
	relacao="municipios_selecionados",
	cte_resultado="municipios_selecionados_filtrados"
) }},

{{ limitar_quantidade_meses(
	relacao="municipios_selecionados_filtrados",
    coluna_data="periodo_data_inicio",
	cte_resultado="final"
) }}

SELECT * FROM final
