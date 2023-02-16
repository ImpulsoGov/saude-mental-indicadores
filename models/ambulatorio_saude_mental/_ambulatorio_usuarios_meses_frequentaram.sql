{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
procedimentos AS (
    SELECT * FROM {{ ref('ambulatorio_atendimentos') }}
),

usuarios_meses_frequentaram AS (
    SELECT
    DISTINCT ON (
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        realizacao_periodo_data_inicio
    )
    *
    FROM procedimentos
    WHERE quantidade_apresentada > 0
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
    AND age(
        -- registros que já foram carregados na última transformação
        ( SELECT max({{this}}.periodo_data_inicio) FROM {{this}} ),
        -- todos os registros na tabela de procedimentos em CAPS
        procedimentos.realizacao_periodo_data_inicio
    ) <= '3 mon 3 days'::interval
    {%- endif %}
    ORDER BY
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        realizacao_periodo_data_inicio DESC,
        -- A ordenação ascendente na coluna de diagnóstico faz com que códigos
        -- que não são "Outras condições não especificadas" (ex.: F99) sejam
        -- preteridos em relação a outros mais específicos, que costumam
        -- terminar em números menores (ex.: F20, F30...).
        condicao_principal_id_cid10 ASC,
        quantidade_apresentada DESC
),

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
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        usuario_nascimento_data,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        now() AS atualizacao_data
    FROM usuarios_meses_frequentaram
)
SELECT * FROM final
