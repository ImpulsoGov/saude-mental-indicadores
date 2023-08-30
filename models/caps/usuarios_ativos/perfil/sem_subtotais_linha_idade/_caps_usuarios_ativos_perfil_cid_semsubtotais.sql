{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_ativos_perfil AS (
    SELECT * FROM {{ ref("_caps_usuarios_ativos_perfil_semsubtotais") }}
),

-- Calcula subtotais para todos os estabelecimentos
{{ calcular_subtotais(
    relacao="usuarios_ativos_perfil",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio",
        "grupo_id_cid10",
        "usuario_condicao_saude"
    ],
    colunas_a_totalizar=[
		"estabelecimento_linha_perfil",
    	"estabelecimento_linha_idade",
		"estabelecimento_id_scnes"
    ],
    nomes_categorias_com_totais=["Todos", "Todos", "0000000"],
    agregacoes_valores={
        "ativos_mes": "sum",
        "ativos_3meses": "sum",
        "tornandose_inativos": "sum",
        "atualizacao_data": "max"
    },
    manter_original=true,
    cte_resultado="usuario_perfil_incluindo_totais"
) }},
{{  revelar_combinacoes_implicitas(
    relacao="usuario_perfil_incluindo_totais",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "estabelecimento_linha_perfil",
        "estabelecimento_linha_idade"
    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["estabelecimento_id_scnes"],
        ["grupo_id_cid10"],
        ["usuario_condicao_saude"]
    ],
    cte_resultado="com_combinacoes"
) }},
{{  remover_subtotais(
    relacao="com_combinacoes",
    cte_resultado="com_combinacoes_sem_subtotais"
) }},

cids_zerados_por_municipio AS (
    SELECT
	    unidade_geografica_id_sus,
	    usuario_condicao_saude
    FROM com_combinacoes_sem_subtotais
    WHERE ativos_mes IS NOT NULL AND ativos_3meses IS NOT NULL AND tornandose_inativos IS NOT NULL
    GROUP BY 
	    unidade_geografica_id_sus,
	    usuario_condicao_saude
),

com_combinacoes_sem_subtotais_sem_cids_zerados AS (
    SELECT TOrg.*
    FROM com_combinacoes_sem_subtotais TOrg
    INNER JOIN cids_zerados_por_municipio CZ
    ON CZ.usuario_condicao_saude = TOrg.usuario_condicao_saude AND CZ.unidade_geografica_id_sus = TOrg.unidade_geografica_id_sus
),

final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
		    "estabelecimento_linha_perfil",
    	    "estabelecimento_linha_idade",
            "estabelecimento_id_scnes",
            "periodo_id",
            "usuario_condicao_saude"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
		estabelecimento_linha_perfil,
		estabelecimento_linha_idade,
        estabelecimento_id_scnes,
        usuario_condicao_saude,
        sum(coalesce(ativos_mes, 0)) AS ativos_mes,
        sum(coalesce(ativos_3meses, 0)) AS ativos_3meses,
        sum(coalesce(tornandose_inativos, 0)) AS tornandose_inativos,
        now() AS atualizacao_data
    FROM com_combinacoes_sem_subtotais_sem_cids_zerados
    GROUP BY    
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,        
        estabelecimento_linha_perfil,
    	estabelecimento_linha_idade,
        estabelecimento_id_scnes,
        usuario_condicao_saude
)
SELECT * FROM final