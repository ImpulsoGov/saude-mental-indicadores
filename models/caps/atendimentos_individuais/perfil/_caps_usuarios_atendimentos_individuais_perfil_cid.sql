{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_atendimendos_individuais_perfil AS (
    SELECT * FROM {{ ref("_caps_usuarios_atendimentos_individuais_perfil_semsubtotais") }}
),
{{ calcular_subtotais(
    relacao="usuarios_atendimendos_individuais_perfil",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio",        
        "usuario_condicao_saude"
    ],
    colunas_a_totalizar=[
		"estabelecimento_linha_perfil",
    	"estabelecimento_linha_idade",
		"estabelecimento_id_scnes"
    ],
    nomes_categorias_com_totais=["Todos", "Todos", "0000000"],
    agregacoes_valores={
        "usuarios_apenas_atendimento_individual": "sum",
        "usuarios_frequentantes": "sum"
    },
    manter_original=true,
    cte_resultado="perfil_incluindo_totais"
) }},
{{  revelar_combinacoes_implicitas(
    relacao="perfil_incluindo_totais",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "estabelecimento_linha_perfil",
        "estabelecimento_linha_idade"
    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["estabelecimento_id_scnes"],
        ["usuario_condicao_saude"]
    ],
    cte_resultado="com_combinacoes"
) }},
{{  remover_subtotais(
    relacao="com_combinacoes",
    cte_resultado="com_combinacoes_sem_subtotais"
) }},
-- Passo necessário para remover CIDs que não apareceram nenhuma vez em nenhuma competência no município
cids_zerados_por_municipio AS (
    SELECT
	    unidade_geografica_id_sus,
	    usuario_condicao_saude
    FROM com_combinacoes_sem_subtotais
    WHERE usuarios_apenas_atendimento_individual IS NOT NULL AND usuarios_frequentantes IS NOT NULL
    GROUP BY 
	    unidade_geografica_id_sus,
	    usuario_condicao_saude
),
com_combinacoes_sem_subtotais_sem_cids_zerados AS (
    SELECT comb_semsub.*
    FROM com_combinacoes_sem_subtotais comb_semsub
    INNER JOIN cids_zerados_por_municipio cids_zerados
    ON cids_zerados.usuario_condicao_saude = comb_semsub.usuario_condicao_saude 
    AND cids_zerados.unidade_geografica_id_sus = comb_semsub.unidade_geografica_id_sus
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
        coalesce(usuarios_apenas_atendimento_individual, 0) AS usuarios_apenas_atendimento_individual,
        coalesce(usuarios_frequentantes, 0) AS usuarios_frequentantes,
        now() AS atualizacao_data   
    FROM com_combinacoes_sem_subtotais_sem_cids_zerados
)
SELECT * FROM final