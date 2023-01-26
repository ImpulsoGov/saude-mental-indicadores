{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
periodos AS (
    SELECT * FROM {{ source('codigos', 'periodos') }}
),
caps_procedimentos AS (
    SELECT * FROM {{ ref('caps_procedimentos_individualizaveis') }}
),
caps_usuarios_atendidos AS (
    SELECT * FROM {{ ref("_raps_usuarios_atendidos") }}
    WHERE estabelecimento_tipo_id_sigtap = '70'
),
caps_usuarios_vinculos AS (
    SELECT * FROM {{ ref('caps_usuarios_vinculos') }}
),
caps_usuarios_meses_todos AS (
    SELECT
        periodos.id AS periodo_id,
        periodos.data_inicio AS periodo_data_inicio,
        caps_usuarios_atendidos.unidade_geografica_id_sus,
        caps_usuarios_atendidos.unidade_geografica_id,
        caps_usuarios_vinculos.estabelecimento_id_scnes,
        caps_usuarios_vinculos.usuario_id_cns_criptografado,
        caps_usuarios_atendidos.usuario_nascimento_data,
        caps_usuarios_atendidos.usuario_sexo_id_sigtap,
        caps_usuarios_atendidos.condicao_principal_id_cid10,
        caps_usuarios_atendidos.usuario_raca_cor_id_siasus,
        caps_usuarios_atendidos.usuario_situacao_rua,
        caps_usuarios_atendidos.usuario_abuso_substancias,
        caps_usuarios_vinculos.acolhimento_periodo_data_inicio
            AS usuario_acolhimento_periodo_data_inicio,
        caps_usuarios_vinculos.primeiro_procedimento_periodo_data_inicio
            AS usuario_primeiro_procedimento_periodo_data_inicio,
        (
            CASE
                WHEN (
                    caps_usuarios_atendidos.periodo_data_inicio
                    >= caps_usuarios_vinculos.primeiro_procedimento_periodo_data_inicio
                ) THEN caps_usuarios_atendidos.periodo_data_inicio
                -- Desconsidera como "último procedimento" os acolhimentos
                -- iniciais
                ELSE NULL
            END
        ) AS usuario_ultimo_procedimento_periodo_data_inicio
    FROM periodos
    -- Combinações entre usuários com vínculos com CAPS e meses disponíveis
    CROSS JOIN caps_usuarios_vinculos
    -- Obtem dados de cadastro dos usuários consolidados mês a mês
    LEFT JOIN caps_usuarios_atendidos
        ON periodos.data_inicio = caps_usuarios_atendidos.periodo_data_inicio
            AND periodos.id = caps_usuarios_atendidos.periodo_id
            AND caps_usuarios_vinculos.estabelecimento_id_scnes
                = caps_usuarios_atendidos.estabelecimento_id_scnes
            AND caps_usuarios_vinculos.usuario_id_cns_criptografado
                = caps_usuarios_atendidos.usuario_id_cns_criptografado
    WHERE periodos.tipo = 'Mensal'
        -- Restringe as combinações dos meses disponíveis com os usuários 
        -- ativos apenas ao período após o acolhimento ou primeiro
        -- procedimento (o que vier primeiro)...
        AND periodos.data_inicio >= least(
            caps_usuarios_vinculos.acolhimento_periodo_data_inicio,
            caps_usuarios_vinculos.primeiro_procedimento_periodo_data_inicio
        )
        -- ...e o quarto mês após o último procedimento realizado
        AND periodos.data_inicio <= (
            ultimo_procedimento_periodo_data_inicio + '4 mon'::interval 
        )
        -- Usuários que nunca realizaram um procedimento (exceto acolhimento 
        -- inicial) não são considerados
        AND ultimo_procedimento_periodo_data_inicio IS NOT NULL
),
{{ preencher_ultimo_nao_nulo(
    relacao="caps_usuarios_meses_todos",
    agrupar_por=[
        "estabelecimento_id_scnes",
        "usuario_id_cns_criptografado"
    ],
    ordenar_por=[
        "periodo_data_inicio",
        "periodo_id"
    ],
    colunas_a_preencher=[
        "unidade_geografica_id_sus",
        "unidade_geografica_id",
        "usuario_nascimento_data",
        "usuario_sexo_id_sigtap",
        "condicao_principal_id_cid10",
        "usuario_raca_cor_id_siasus",
        "usuario_situacao_rua",
        "usuario_abuso_substancias",
        "usuario_acolhimento_periodo_data_inicio",
        "usuario_primeiro_procedimento_periodo_data_inicio",
        "usuario_ultimo_procedimento_periodo_data_inicio"
    ],
    colunas_manter=[],
    cte_resultado="caps_usuarios_meses_todos_preenchido"
) }},
{{ ultimas_competencias(
    relacao="caps_usuarios_meses_todos_preenchido",
    fontes=[
        "bpa_i_disseminacao",
        "raas_psicossocial_disseminacao"
    ],
    meses_antes_ultima_competencia=(0, none),
    cte_resultado="ate_ultima_competencia"
) }},
{{ classificar_tempo_servico(
    relacao="ate_ultima_competencia",
    coluna_primeiro_procedimento=(
        "usuario_primeiro_procedimento_periodo_data_inicio"
    ),
    coluna_data_referencia="periodo_data_inicio",
    cte_resultado="com_tempo_servico",
) }},
{{ classificar_faixa_etaria(
    relacao="com_tempo_servico",
    coluna_nascimento_data="usuario_nascimento_data",
    coluna_data_referencia="periodo_data_inicio",
    idade_tipo="Anos",
    faixa_etaria_agrupamento="10 em 10 anos",
    colunas_faixa_etaria=["id", "descricao", "ordem"],
    cte_resultado="com_faixa_etaria"
) }},
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "estabelecimento_id_scnes",
            "periodo_id",
            "usuario_id_cns_criptografado"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        faixa_etaria_id AS usuario_faixa_etaria_id,
        faixa_etaria_descricao AS usuario_faixa_etaria_descricao,
        faixa_etaria_ordem AS usuario_faixa_etaria_ordem,
        extract(YEAR FROM age(periodo_data_inicio, usuario_nascimento_data))
            AS usuario_idade,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias,
        usuario_acolhimento_periodo_data_inicio,
        usuario_primeiro_procedimento_periodo_data_inicio,
        usuario_ultimo_procedimento_periodo_data_inicio,
        tempo_servico_id AS usuario_tempo_servico_id,
        tempo_servico_descricao AS usuario_tempo_servico_descricao,
        tempo_servico_ordem AS usuario_tempo_servico_ordem,
        coalesce(
            periodo_data_inicio
            = usuario_ultimo_procedimento_periodo_data_inicio,
            FALSE
        ) AS ativo_mes,
        -- Os usuários são considerados ativos durante três meses a partir do
        -- último procedimento realizado
        coalesce(
            age(
                periodo_data_inicio,
                usuario_ultimo_procedimento_periodo_data_inicio
            ) < '3 mon'::interval,
            FALSE
        ) AS ativo_3meses,
        -- No terceiro mês seguido sem movimentação no prontuário, assume-se
        -- usuário está se tornando inativo.
        coalesce(
            (
                age(
                    periodo_data_inicio,
                    usuario_ultimo_procedimento_periodo_data_inicio 
                ) >= '3 mon'::interval
                AND age(
                    periodo_data_inicio,
                    usuario_ultimo_procedimento_periodo_data_inicio
                ) < '4 mon'::interval
            ),
            FALSE
        ) AS tornandose_inativo,
        now() AS atualizacao_data
    FROM com_faixa_etaria usuario
)
SELECT * FROM final
