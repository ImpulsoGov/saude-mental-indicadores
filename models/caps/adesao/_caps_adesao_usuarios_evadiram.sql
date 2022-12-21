{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
usuarios_atividade_ambulatorio AS (
    SELECT * FROM {{ ref("_ambulatorio_usuarios_meses_frequentaram") }}
    {%- if is_incremental() %}
        {#
        No modo incremental, são considerados os procedimentos realizados nos cinco meses anteriores ao mês da última atualização do modelo.
        
        No caso de mudanças nas regras de negócio de modelo ou de adições
        de novas unidades geográficas, o modelo deve ser executado com a opção
        `--full-refresh` para abranger todos os usuários.
        #}
        WHERE periodo_data_inicio
            > date_trunc(
                'month',
                (SELECT max(atualizacao_data) FROM {{ this }})
            ) - '5 months'::interval
    {%- endif %}
),

usuarios_atividade_caps AS (
    SELECT * FROM {{ ref("caps_usuarios_ativos") }}
    {%- if is_incremental() %}
        {#
        No modo incremental, são considerados os procedimentos realizados nos cinco meses anteriores ao mês da última atualização do modelo.
        
        No caso de mudanças nas regras de negócio de modelo ou de adições
        de novas unidades geográficas, o modelo deve ser executado com a opção
        `--full-refresh` para abranger todos os usuários.
        #}
        WHERE periodo_data_inicio
            > date_trunc(
                'month',
                (SELECT max(atualizacao_data) FROM {{ this }})
            ) - '5 months'::interval
    {%- endif %}
),

usuarios_perfil_ambulatorial AS (
    SELECT * FROM {{ ref("_caps_usuarios_perfil_ambulatorial") }}
),

usuarios_recentes_nao_aderiram AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        min(usuario_primeiro_procedimento_periodo_data_inicio)
            AS usuario_primeiro_procedimento_periodo_data_inicio,
        -- Considera-se como data de referência da evasão o início do mês
        -- seguinte ao último em que houve movimentação nas fichas de 
        -- procedimentos
        (
            '1 mon'::interval + max(ultimo_procedimento_periodo_data_inicio)
            FILTER (WHERE tornandose_inativo)
        )::date AS evadiu_a_partir_de_periodo_data_inicio,
        bool_or(tornandose_inativo) AS evadiu
    FROM usuarios_atividade_caps
    -- Filtra o período em que o usuário é recente na definição adotada
    -- no cálculo do indicador (atualmente, 3 meses), mais 2 meses para 
    -- verificar períodos de inatividade iniciados nos últimos dois meses
    -- do período de referência. Por exemplo: se uma pessoa deixou de 
    -- frequentar o CAPS no 3º mês após o primeiro procedimento, só no 5º mês
    -- ela aparecerá como "tornando-se inativa" na tabela de usuários ativos.
    WHERE periodo_data_inicio >=
        usuario_primeiro_procedimento_periodo_data_inicio
        AND periodo_data_inicio <= (
            usuario_primeiro_procedimento_periodo_data_inicio
            + '5 mon'::interval
        )
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado
),

-- Exclui da conta usuários que estão no CAPS mas foram classificados como
-- tendo perfil para ambulatório
exceto_perfil_ambulatorial AS (
    SELECT
        usuarios_recentes_nao_aderiram.*
    FROM usuarios_recentes_nao_aderiram
    LEFT JOIN usuarios_perfil_ambulatorial
        ON usuarios_recentes_nao_aderiram.estabelecimento_id_scnes
            = usuarios_perfil_ambulatorial.estabelecimento_id_scnes
        AND usuarios_recentes_nao_aderiram.usuario_id_cns_criptografado
            = usuarios_perfil_ambulatorial.usuario_id_cns_criptografado
    WHERE usuarios_perfil_ambulatorial.id IS NULL
),

-- Exclui da conta usuários que foram atendidos em um ambulatório de saúde
-- mental em até três meses após parar de frequentar um CAPS
exceto_encaminhamentos_ambulatorio AS (
    SELECT
        exceto_perfil_ambulatorial.*
    FROM exceto_perfil_ambulatorial
    LEFT JOIN usuarios_atividade_ambulatorio
        ON exceto_perfil_ambulatorial.usuario_id_cns_criptografado
            = usuarios_atividade_ambulatorio.usuario_id_cns_criptografado
        AND usuarios_atividade_ambulatorio.periodo_data_inicio
            >=
            exceto_perfil_ambulatorial.evadiu_a_partir_de_periodo_data_inicio
        AND usuarios_atividade_ambulatorio.periodo_data_inicio
            <= 
            exceto_perfil_ambulatorial.evadiu_a_partir_de_periodo_data_inicio
            + '2 mon'::interval
    WHERE usuarios_atividade_ambulatorio.id IS NULL
),

{# -- Exclui as 4 competências mais recentes, já que nelas ainda não houve tempo 
-- para observar o comportamento (adesão/evasão) nos três meses iniciais após
-- o primeiro procedimento, mais os 2 meses necessários para confirmar as
-- evasões ocorridas nas últimas competências dentro desse período 
{{ ultimas_competencias(
    relacao="exceto_perfil_ambulatorial",
    fontes=["bpa_i_disseminacao", "raas_psicossocial_disseminacao"],
    meses_antes_ultima_competencia=(4, none),
    cte_resultado="exceto_ultimas_4_competencias"
) }}, #}

final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "estabelecimento_id_scnes",
            "usuario_id_cns_criptografado"
        ]) }} AS id,
        *,
        now() AS atualizacao_data
    {# FROM exceto_ultimas_4_competencias #}
    FROM exceto_perfil_ambulatorial
)

SELECT * FROM final
