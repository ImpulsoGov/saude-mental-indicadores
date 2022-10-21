{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
usuarios_ativos AS (
    SELECT *
    FROM {{ ref("caps_usuarios_ativos") }}
),
condicoes_saude AS (
    SELECT 
        DISTINCT ON (id_cid10)
        *
    FROM {{ source("codigos", "condicoes_saude") }}
    ORDER BY id_cid10
),
por_estabelecimentos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_tempo_servico_id,
        usuario_tempo_servico_descricao,
        usuario_tempo_servico_ordem,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria_descricao,
        usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        condicoes_saude.grupo_id_cid10,
        condicoes_saude.grupo_descricao_curta_cid10 AS usuario_condicao_saude,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias,
        count (DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE ativo_mes
        ) AS ativos_mes,
        count (DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE ativo_3meses
        ) AS ativos_3meses,
        count (DISTINCT usuario_id_cns_criptografado) FILTER (
            WHERE tornandose_inativo
        ) AS tornandose_inativos,
        max(atualizacao_data) AS atualizacao_data
    FROM usuarios_ativos
    LEFT JOIN condicoes_saude
    ON usuarios_ativos.condicao_principal_id_cid10 = condicoes_saude.id_cid10
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_tempo_servico_id,
        usuario_tempo_servico_descricao,
        usuario_tempo_servico_ordem,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria_descricao,
        usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        condicoes_saude.grupo_id_cid10,
        condicoes_saude.grupo_descricao_curta_cid10,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias
),
{{ classificar_caps_linha(
    relacao="por_estabelecimentos",
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id=none,
    todas_linhas_valor=none,
    cte_resultado="por_estabelecimento_com_linhas_cuidado"
) }},
{{ calcular_subtotais(
    relacao="por_estabelecimento_com_linhas_cuidado",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio",
        "usuario_tempo_servico_id",
        "usuario_tempo_servico_descricao",
        "usuario_tempo_servico_ordem",
        "usuario_faixa_etaria_id",
        "usuario_faixa_etaria_descricao",
        "usuario_faixa_etaria_ordem",
        "usuario_sexo_id_sigtap",
        "grupo_id_cid10",
        "usuario_condicao_saude",
        "usuario_raca_cor_id_siasus",
        "usuario_situacao_rua",
        "usuario_abuso_substancias"
    ],
    colunas_a_totalizar=[
		"estabelecimento_linha_perfil",
    	"estabelecimento_linha_idade",
		"estabelecimento_id_scnes"
    ],
    nomes_categorias_com_totais=["0000000"],
    agregacoes_valores={
        "ativos_mes": "sum",
        "ativos_3meses": "sum",
        "tornandose_inativos": "sum",
        "atualizacao_data": "max"
    },
    manter_original=true,
    cte_resultado="usuario_perfil_incluindo_totais"
) }},
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
		    "estabelecimento_linha_perfil",
    	    "estabelecimento_linha_idade",
            "estabelecimento_id_scnes",
            "periodo_id",
            "usuario_tempo_servico_id",
            "usuario_faixa_etaria_id",
            "usuario_sexo_id_sigtap",
            "grupo_id_cid10",
            "usuario_raca_cor_id_siasus",
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
        usuario_tempo_servico_descricao AS usuario_tempo_servico,
        usuario_tempo_servico_ordem,
        usuario_faixa_etaria_descricao AS usuario_faixa_etaria,
        usuario_faixa_etaria_ordem,
        usuario_sexo_id_sigtap,
        usuario_condicao_saude,
        usuario_raca_cor_id_siasus,
        saude_mental.classificar_binarios(
            usuario_situacao_rua
        ) AS usuario_situacao_rua,
        saude_mental.classificar_binarios(
            usuario_abuso_substancias
        ) AS usuario_abuso_substancias,
        ativos_mes,
        ativos_3meses,
        tornandose_inativos,
        now() AS atualizacao_data
    FROM usuario_perfil_incluindo_totais usuario_perfil
)
SELECT * FROM final
