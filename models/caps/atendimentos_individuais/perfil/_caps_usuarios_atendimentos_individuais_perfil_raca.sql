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
        "usuario_raca_cor_id_siasus",
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
        ["usuario_raca_cor_id_siasus"]
    ],
    cte_resultado="com_combinacoes"
) }},
{{  remover_subtotais(
    relacao="com_combinacoes",
    cte_resultado="com_combinacoes_sem_subtotais"
) }},
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
		    "estabelecimento_linha_perfil",
    	    "estabelecimento_linha_idade",
            "estabelecimento_id_scnes",
            "periodo_id",
            "usuario_raca_cor_id_siasus"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_linha_perfil,
        estabelecimento_linha_idade,
        estabelecimento_id_scnes,
        usuario_raca_cor_id_siasus, 
        coalesce(usuarios_apenas_atendimento_individual, 0) AS usuarios_apenas_atendimento_individual,
        coalesce(usuarios_frequentantes, 0) AS usuarios_frequentantes,
        now() AS atualizacao_data   
    FROM com_combinacoes_sem_subtotais
)
SELECT * FROM final