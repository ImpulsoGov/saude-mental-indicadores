{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

WITH
usuarios_novos_perfil AS (
    SELECT * FROM {{ ref('_caps_usuarios_novos_perfil_semsubtotais') }}
),
{{ calcular_subtotais(
    relacao="usuarios_novos_perfil",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio",        
        "usuario_situacao_rua",
        "usuario_abuso_substancias"
    ],
    colunas_a_totalizar=[
		"estabelecimento_linha_perfil",
    	"estabelecimento_linha_idade",
		"estabelecimento_id_scnes"
    ],
    nomes_categorias_com_totais=["Todos", "Todos", "0000000"],
    agregacoes_valores={
        "usuarios_novos": "sum",
        "usuarios_novos_anterior": "sum"
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
        ["usuario_situacao_rua"],
        ["usuario_abuso_substancias"]
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
            "usuario_situacao_rua",
            "usuario_abuso_substancias"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_linha_perfil,
        estabelecimento_linha_idade,
        estabelecimento_id_scnes,
        {{ classificar_coluna_binaria(
            coluna="com_combinacoes_sem_subtotais.usuario_situacao_rua",
        ) }} AS usuario_situacao_rua,
        {{ classificar_coluna_binaria(
            coluna="com_combinacoes_sem_subtotais.usuario_abuso_substancias",
        ) }} AS usuario_abuso_substancias,    
        coalesce(com_combinacoes_sem_subtotais.usuarios_novos, 0) AS usuarios_novos,
        coalesce(com_combinacoes_sem_subtotais.usuarios_novos_anterior, 0) AS usuarios_novos_anterior,
        (
            coalesce(com_combinacoes_sem_subtotais.usuarios_novos, 0)
            - coalesce(com_combinacoes_sem_subtotais.usuarios_novos_anterior, 0)
        ) AS dif_usuarios_novos_anterior,
        now() AS atualizacao_data  
    FROM com_combinacoes_sem_subtotais
)
SELECT * FROM final