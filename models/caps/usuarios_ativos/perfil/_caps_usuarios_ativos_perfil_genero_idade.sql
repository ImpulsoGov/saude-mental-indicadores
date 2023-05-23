{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_ativos_perfil AS (
    SELECT * FROM {{ ref("_caps_usuarios_ativos_perfil") }}
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
        "usuario_faixa_etaria_id",
        "usuario_faixa_etaria_descricao",
        "usuario_faixa_etaria_ordem",
        "usuario_sexo_id_sigtap"
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
-- -- Separa apenas o estabelecimento Todos (0000000)
-- usuario_perfil_incluindo_apenas_totais AS (
--     SELECT *
--     FROM usuario_perfil_incluindo_totais
--     WHERE estabelecimento_id_scnes = '0000000'
-- ),
-- -- separa tudo que não é todos e tira as totalizações por linha e idade. Não seria um passo necessário utilizando a CTE 'por_estabelecimento_com_linhas_cuidado', mas foi preciso porque com essa opção ocorria um erro com os tipos das colunas que são UUID
-- usuario_perfil_excluindo_totais AS (
--     SELECT *
--     FROM usuario_perfil_incluindo_totais
--     WHERE estabelecimento_id_scnes <> '0000000' AND estabelecimento_linha_idade <> 'Todos' AND estabelecimento_linha_perfil <> 'Todos'
-- ),
-- -- Junta com demais estabelecimentos sem o cálculo de subtotais por linha e idade de cuidado
-- usuario_perfil_totais_e_individuais AS (
--     SELECT * FROM usuario_perfil_incluindo_apenas_totais
--     UNION
--     SELECT * FROM usuario_perfil_excluindo_totais
-- ),
{{  revelar_combinacoes_implicitas(
    relacao="usuario_perfil_incluindo_totais",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["estabelecimento_id_scnes"],
        ["estabelecimento_linha_perfil"],
    	["estabelecimento_linha_idade"],
        ["usuario_faixa_etaria_id", "usuario_faixa_etaria_descricao", "usuario_faixa_etaria_ordem"],
        ["usuario_sexo_id_sigtap"]
    ],
    cte_resultado="com_combinacoes"
) }},
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
		    "estabelecimento_linha_perfil",
    	    "estabelecimento_linha_idade",
            "estabelecimento_id_scnes",
            "periodo_id",
            "usuario_faixa_etaria_id",
            "usuario_sexo_id_sigtap"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
		estabelecimento_linha_perfil,
		estabelecimento_linha_idade,
        estabelecimento_id_scnes,
        -- usuario_faixa_etaria_id,
        usuario_faixa_etaria_descricao AS usuario_faixa_etaria,
        -- usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        sum(coalesce(ativos_mes, 0)) AS ativos_mes,
        sum(coalesce(ativos_3meses, 0)) AS ativo_3meses,
        sum(coalesce(tornandose_inativos, 0)) AS tornandose_inativos,
        now() AS atualizacao_data
    FROM com_combinacoes 
    GROUP BY    
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,        
        estabelecimento_linha_perfil,
    	estabelecimento_linha_idade,
        estabelecimento_id_scnes,
        usuario_faixa_etaria_descricao,
        usuario_sexo_id_sigtap
)
SELECT * FROM final