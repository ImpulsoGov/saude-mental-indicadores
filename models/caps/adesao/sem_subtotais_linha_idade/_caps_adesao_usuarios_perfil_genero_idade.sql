{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
adesao_usuarios_perfil AS (
    SELECT * FROM {{ ref("_caps_adesao_usuarios_perfil_semsubtotais") }}
),

{{ calcular_subtotais(
    relacao="adesao_usuarios_perfil",
    agrupar_por=[
        "periodo_id",
        "periodo_data_inicio",
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "usuario_faixa_etaria_id",
        "usuario_faixa_etaria_descricao",
        "usuario_faixa_etaria_ordem",
        "usuario_sexo_id_sigtap",
        "estatus_adesao_mes",
        "estatus_adesao_final"
    ],
    colunas_a_totalizar=[
		"estabelecimento_id_scnes"
    ],
    nomes_categorias_com_totais=["0000000"],
    agregacoes_valores={
        "quantidade_registrada": "sum"
    },
    manter_original=true,
    cte_resultado="perfil_incluindo_totais"
) }},

{{  revelar_combinacoes_implicitas(
    relacao="perfil_incluindo_totais",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus"
    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["estabelecimento_id_scnes"],
        ["usuario_faixa_etaria_id", "usuario_faixa_etaria_descricao", "usuario_faixa_etaria_ordem"],
        ["usuario_sexo_id_sigtap"],
        ["estatus_adesao_mes"],
        ["estatus_adesao_final"]
    ],
    cte_resultado="com_combinacoes"
) }},

{{ classificar_caps_linha(
    relacao="com_combinacoes",
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id="0000000",
    todas_linhas_valor="Todos",
    cte_resultado="com_linhas_cuidado"
) }},

final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "periodo_id",
            "unidade_geografica_id",
            "estabelecimento_id_scnes",
            "usuario_faixa_etaria_id",
            "usuario_sexo_id_sigtap",
            "estatus_adesao_mes",
            "estatus_adesao_final"
        ])}} AS id,
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
        estatus_adesao_mes,
        estatus_adesao_final,
        coalesce(quantidade_registrada, 0) AS quantidade_registrada,
        now() AS atualizacao_data   
    FROM com_linhas_cuidado
)
SELECT * FROM final



