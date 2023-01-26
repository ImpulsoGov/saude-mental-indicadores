{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
procedimentos_individuais AS (
    SELECT * FROM {{ ref("procedimentos_individuais") }}
),

procedimentos_realizados AS (
    SELECT * FROM {{ ref("caps_procedimentos_individualizaveis") }}
    {%- if is_incremental() %}
        {#
        No modo incremental, são considerados os procedimentos realizados nos três meses anteriores ao mês da última atualização do modelo.
        
        No caso de mudanças nas regras de negócio de modelo ou de adições
        de novas unidades geográficas, o modelo deve ser executado com a opção
        `--full-refresh` para abranger todos os usuários.
        #}
        WHERE periodo_data_inicio
            > date_trunc(
                'month',
                (SELECT max(atualizacao_data) FROM {{ this }})
            ) - '3 months'::interval
    {%- endif %}
),

usuarios_caps AS (
    SELECT * FROM {{ ref("caps_usuarios_ativos") }}
    {%- if is_incremental() %}
        {#
        No modo incremental, são atualizados os usuários que tiveram o primeiro procedimento registrado no estabelecimento nos três meses
        anteriores ao mês da última atualização do modelo.
        
        No caso de mudanças nas regras de negócio de modelo ou de adições de
        novas unidades geográficas, o modelo deve ser executado com a opção
        `--full-refresh` para abranger todos os usuários.
        #}
        WHERE usuario_primeiro_procedimento_periodo_data_inicio
            > date_trunc(
                'month',
                (SELECT max(atualizacao_data) FROM {{ this }})
            ) - '3 months'::interval
    {%- endif %}
),

usuarios_perfil_ambulatorial AS (
    SELECT
        usuarios_caps.estabelecimento_id_scnes,
        usuarios_caps.usuario_id_cns_criptografado
    FROM usuarios_caps
    LEFT JOIN procedimentos_realizados
        -- Usuário com o mesmo Cartão Nacional de Saúde
        ON usuarios_caps.usuario_id_cns_criptografado
            = procedimentos_realizados.usuario_id_cns_criptografado
            -- Atendido no mesmo estabelecimento de saúde
            AND usuarios_caps.estabelecimento_id_scnes
            = procedimentos_realizados.estabelecimento_id_scnes
            -- Procedimento após o primeiro procedimento do usuário no
            -- estabelecimento
            AND procedimentos_realizados.periodo_data_inicio
            >= usuarios_caps.usuario_primeiro_procedimento_periodo_data_inicio
            -- ...e até o terceiro mês após o primeiro procedimento
            AND procedimentos_realizados.periodo_data_inicio
            < usuarios_caps.usuario_primeiro_procedimento_periodo_data_inicio
            + '3 mon'::interval
    LEFT JOIN procedimentos_individuais
        ON procedimentos_realizados.procedimento_id_sigtap
            = procedimentos_individuais.procedimento_id_sigtap
    WHERE
        procedimentos_realizados.quantidade_apresentada > 0
    -- Consolida por combinação de usuário e estabelecimento
    GROUP BY
        usuarios_caps.estabelecimento_id_scnes,
        usuarios_caps.usuario_id_cns_criptografado
    -- Verifica se os procedimentos realizados no período batem com a lista de
    -- procedimentos de caráter individual/ambulatorial, de forma a
    -- caracterizar que o usuário teria o perfil para estar em ambulatorio, não
    -- em CAPS
    HAVING bool_and(
        procedimentos_individuais.procedimento_id_sigtap IS NOT NULL
    )
),

final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "estabelecimento_id_scnes",
            "usuario_id_cns_criptografado"
        ]) }} AS id,
        usuarios_perfil_ambulatorial.*,
        now() AS atualizacao_data
    FROM usuarios_perfil_ambulatorial
)

SELECT * FROM final
