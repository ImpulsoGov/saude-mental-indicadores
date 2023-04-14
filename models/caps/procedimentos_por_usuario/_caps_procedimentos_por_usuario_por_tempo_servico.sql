{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
procedimentos_usuario_mes AS (
    SELECT * FROM {{ ref("_caps_procedimentos_por_usuario_por_mes") }}
),
por_tempo_servico_por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        competencia,
        periodo_id,
        tempo_servico_descricao,
        round(
            (
                sum(procedimentos_exceto_acolhimento)::numeric
                / nullif(count(DISTINCT usuario_id_cns_criptografado), 0)
            ),
            1
        ) AS procedimentos_por_usuario
    FROM procedimentos_usuario_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        competencia,
        periodo_id,
        tempo_servico_descricao
),
por_tempo_servico_todos_estabelecimentos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        '0000000' AS estabelecimento_id_scnes,
        competencia,
        periodo_id,
        tempo_servico_descricao,
        round(
            (
                sum(procedimentos_exceto_acolhimento)::numeric
                / nullif(count(DISTINCT usuario_id_cns_criptografado), 0)
            ),
            1
        ) AS procedimentos_por_usuario
    FROM procedimentos_usuario_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        tempo_servico_descricao
),
por_tempo_servico AS (
    SELECT * FROM por_tempo_servico_por_estabelecimento
    UNION
    SELECT * FROM por_tempo_servico_todos_estabelecimentos
),
{{ classificar_caps_linha(
    relacao="por_tempo_servico",
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id=none,
    todas_linhas_valor=none,
    cte_resultado="com_linhas_cuidado"
) }},
{{ calcular_subtotais(
    relacao="com_linhas_cuidado",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "estabelecimento_id_scnes",
        "competencia",
        "periodo_id",
        "tempo_servico_descricao"
    ],
    colunas_a_totalizar=[
        "estabelecimento_linha_perfil",
        "estabelecimento_linha_idade"
    ],
    nomes_categorias_com_totais=[
        "Todos", 
        "Todos"
    ],
    agregacoes_valores={
        "procedimentos_por_usuario": "max"
    },
    cte_resultado="com_totais"
) }},
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id",
            "estabelecimento_id_scnes",
            "tempo_servico_descricao",
            "estabelecimento_linha_perfil",
            "estabelecimento_linha_idade"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        estabelecimento_id_scnes,
        competencia AS periodo_data_inicio,
        tempo_servico_descricao,
        procedimentos_por_usuario,
        estabelecimento_linha_perfil,
        estabelecimento_linha_idade
    FROM com_totais
)
SELECT * FROM final