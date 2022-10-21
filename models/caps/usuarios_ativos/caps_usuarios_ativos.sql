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
caps_usuarios_primeiro_procedimento AS (
    SELECT * FROM {{ ref('caps_usuarios_primeiro_procedimento') }}
),
caps_usuarios_meses_frequentaram AS (
    SELECT
    DISTINCT ON (
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        periodo_data_inicio
    )
    *,
    -- competência do último procedimento realizado no estabelecimento,
    -- desconsiderando acolhimento inicial.
    (
        CASE 
            WHEN procedimento_id_sigtap NOT IN (
                '0301080232',  -- ACOLHIMENTO INICIAL POR CAPS
                '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (AC DEMANDA ESPONT)
            ) THEN periodo_data_inicio
            ELSE NULL
        END
    ) AS ultimo_procedimento_periodo_data_inicio
    FROM caps_procedimentos
    {%- if is_incremental() %}
    {#
    No modo incremental, seleciona apenas as três últimas competências com
    dados disponíveis para transformar novamente. Com isso, evita-se rodar a
    transformação inteira novamente, ao mesmo tempo em que admite-se correções
    nos arquivos de origem durante três meses após a primeira divulgação. 
    IMPORTANTE: Quando houver inserção de novas unidades geográficas na tabela
    de procedimentos CAPS, é necessário refazer a transformação toda com a
    opção `--full-refresh`.
    #}
    WHERE age(
        -- registros que já foram carregados na última transformação
        ( SELECT max({{this}}.periodo_data_inicio) FROM {{this}} ),
        -- todos os registros na tabela de procedimentos em CAPS
        caps_procedimentos.periodo_data_inicio
    ) <= '3 mon 3 days'::interval
    {%- endif %}
    ORDER BY
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        periodo_data_inicio DESC,
        -- se houver registros de RAAS e de BPA dentro do mesmo mês, prefere os
        -- registros de RAAS, nos quais os campos `usuario_situacao_rua` e 
        -- `usuario_abuso_substancias` NÃO são nulos
        usuario_situacao_rua DESC,
        usuario_abuso_substancias DESC,
        -- o BPA-i acolhimento inicial, se houver, será preterido em relação a
        -- qualquer outro que existir para o mesmo usuário e estabelecimento
        -- naquele mês. Isso porque o acolhimento inicial não é considerado
        -- para a coluna "ultimo_procedimento_realizado"
        procedimento_id_sigtap IN (
            '0301080232',  -- ACOLHIMENTO INICIAL POR CAPS
            '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (AC DEMANDA ESPONT)
        ) DESC,
        -- A ordenação ascendente na coluna de diagnóstico faz com que códigos
        -- que não são "Outras condições não especificadas" (ex.: F99) sejam
        -- preteridos em relação a outros mais específicos, que costumam
        -- terminar em números menores (ex.: F20, F30...).
        condicao_principal_id_cid10 ASC,
        quantidade_apresentada DESC
),
{{ revelar_combinacoes_implicitas(
    relacao="caps_usuarios_meses_frequentaram",
    agrupar_por=[
        "unidade_geografica_id_sus",
        "unidade_geografica_id",
        "estabelecimento_id_scnes"
    ],
    colunas_a_completar=[
        ["periodo_id", "periodo_data_inicio"],
        ["usuario_id_cns_criptografado"]
    ],
    cte_resultado="caps_usuarios_meses_todos"
) }},
{{ preencher_ultimo_nao_nulo(
    relacao="caps_usuarios_meses_todos",
    agrupar_por=[
        "unidade_geografica_id_sus",
        "unidade_geografica_id",
        "estabelecimento_id_scnes",
        "usuario_id_cns_criptografado"
    ],
    ordenar_por=[
        "periodo_data_inicio",
        "periodo_id"
    ],
    colunas_a_preencher=[
        "usuario_nascimento_data",
        "usuario_sexo_id_sigtap",
        "condicao_principal_id_cid10",
        "usuario_raca_cor_id_siasus",
        "usuario_situacao_rua",
        "usuario_abuso_substancias",
        "ultimo_procedimento_periodo_data_inicio"
    ],
    colunas_manter=[],
    cte_resultado="caps_usuarios_meses_todos_preenchido"
) }},
a_partir_primeiro_procedimento AS (
    SELECT
        usuario.*,
        primeiro_procedimento.acolhimento_periodo_data_inicio
            AS usuario_acolhimento_periodo_data_inicio,
        primeiro_procedimento.primeiro_procedimento_periodo_data_inicio
            AS usuario_primeiro_procedimento_periodo_data_inicio
    FROM caps_usuarios_meses_todos_preenchido usuario
    LEFT JOIN caps_usuarios_primeiro_procedimento primeiro_procedimento
    USING (
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado
    )
    WHERE usuario.periodo_data_inicio >= least(
        primeiro_procedimento.acolhimento_periodo_data_inicio,
        primeiro_procedimento.primeiro_procedimento_periodo_data_inicio
    )
),
{{ classificar_tempo_servico(
    relacao="a_partir_primeiro_procedimento",
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
        ultimo_procedimento_periodo_data_inicio,
        tempo_servico_id AS usuario_tempo_servico_id,
        tempo_servico_descricao AS usuario_tempo_servico_descricao,
        tempo_servico_ordem AS usuario_tempo_servico_ordem,
        coalesce(
            periodo_data_inicio = ultimo_procedimento_periodo_data_inicio,
            FALSE
        ) AS ativo_mes,
        -- Os usuários são considerados ativos durante três meses a partir do
        -- último procedimento realizado
        coalesce(
            age(
                periodo_data_inicio,
                ultimo_procedimento_periodo_data_inicio
            ) < '3 mon'::interval,
            FALSE
        ) AS ativo_3meses,
        -- No terceiro mês seguido sem movimentação no prontuário, assume-se
        -- usuário está se tornando inativo.
        coalesce(
            (
                age(
                    periodo_data_inicio,
                    ultimo_procedimento_periodo_data_inicio 
                ) >= '3 mon'::interval
                AND age(
                    periodo_data_inicio,
                    ultimo_procedimento_periodo_data_inicio
                ) < '4 mon'::interval
            ),
            FALSE
        ) AS tornandose_inativo,
        now() AS atualizacao_data
    FROM com_faixa_etaria usuario
)
SELECT * FROM final
