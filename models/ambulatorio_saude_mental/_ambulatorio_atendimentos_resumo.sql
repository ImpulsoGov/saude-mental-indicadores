{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH 
disponibilidade_ocupacao_por_estabelecimento AS (
    SELECT * FROM {{ ref("disponibilidade_ocupacao_por_estabelecimento") }}
),
estabelecimentos AS (
    SELECT * FROM {{ source("codigos", "estabelecimentos") }}
),
ocupacoes AS (
    SELECT * FROM {{ source("codigos", "ocupacoes") }}
),
referencias_atendimentos AS (
    SELECT * FROM {{ ref("ambulatorio_atendimentos") }}
),
atendimentos_por_ocupacao_por_estabelecimento AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
        realizacao_periodo_data_inicio as periodo_data_inicio,
		estabelecimento_id_scnes,
		profissional_vinculo_ocupacao_id_cbo2002 AS ocupacao_id_cbo2002,
		sum(quantidade_apresentada) AS procedimentos_realizados,
        max(atualizacao_data) AS atualizacao_data
	FROM referencias_atendimentos
	GROUP BY 
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
        realizacao_periodo_data_inicio,
		estabelecimento_id_scnes,
		profissional_vinculo_ocupacao_id_cbo2002
),
procedimentos_x_disponibilidade AS (
    SELECT
        procedimentos.*,
        disponibilidade.disponibilidade_mensal
    FROM atendimentos_por_ocupacao_por_estabelecimento procedimentos
    LEFT JOIN disponibilidade_ocupacao_por_estabelecimento disponibilidade
    USING (
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_scnes,
        ocupacao_id_cbo2002
    )
),
{{ calcular_subtotais(
    relacao="procedimentos_x_disponibilidade",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio",
    ],
    colunas_a_totalizar=[
        "estabelecimento_id_scnes",
        "ocupacao_id_cbo2002",
    ],
    nomes_categorias_com_totais=[
        "0000000",
        "000000",
    ],
    agregacoes_valores={
        "procedimentos_realizados": "sum",
        "disponibilidade_mensal": "sum",
        "atualizacao_data": "max"
    },
    manter_original=true,
    cte_resultado="procedimentos_x_disponibilidade_com_totais"
) }},
{{  revelar_combinacoes_implicitas(
    relacao="procedimentos_x_disponibilidade_com_totais",
    agrupar_por=[
        
    ],
    colunas_a_completar=[
        ["unidade_geografica_id", "unidade_geografica_id_sus"],
        ["periodo_id", "periodo_data_inicio"],
        ["estabelecimento_id_scnes"],
        ["ocupacao_id_cbo2002"]
    ],
    cte_resultado="com_combinacoes_vazias_e_repeticoes"
) }},

-- Passo necessário para remover os estabelecimentos não pertencentes ao município
-- que acabaram sendo gerados na etapa anterior. CÓDIGO A REFATORAR ASSIM QUE POSSÍVEL
-- PARA EVITAR ESSE PROBLEMA.

ambulatorios_por_municipio AS (
    SELECT DISTINCT estabelecimento_id_scnes, unidade_geografica_id_sus
    FROM procedimentos_x_disponibilidade_com_totais
),

com_combinacoes_vazias AS (
    SELECT ccver.*
    FROM com_combinacoes_vazias_e_repeticoes ccver
    RIGHT JOIN ambulatorios_por_municipio amb_por_mun
    USING (unidade_geografica_id_sus, estabelecimento_id_scnes)
),

com_procedimentos_por_hora AS (
    SELECT
        *,
		round(
			procedimentos_realizados::numeric
			/ nullif(disponibilidade_mensal, 0)::numeric,
			2
		) AS procedimentos_por_hora
    FROM com_combinacoes_vazias
    {%- if is_incremental() %}
    WHERE atualizacao_data > (
        SELECT max(atualizacao_data) FROM {{ this }}
    )
    {%- endif %}
),
{{ juntar_periodos_consecutivos(
    relacao="com_procedimentos_por_hora",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "estabelecimento_id_scnes",
        "ocupacao_id_cbo2002",
    ],
    colunas_valores=[
        "procedimentos_realizados",
        "procedimentos_por_hora",
    ],
    colunas_adicionais_periodo=["periodo_data_inicio"],
    cte_resultado="com_periodo_anterior"
) }},
{{ ultimas_competencias(
    relacao="com_periodo_anterior",
    fontes=["bpa_i_disseminacao", "vinculos_profissionais"],
    meses_antes_ultima_competencia=(0, none),
    cte_resultado="ate_ultima_competencia"
) }},
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id",
            "estabelecimento_id_scnes",
            "ocupacao_id_cbo2002"
        ]) }} AS id,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        ocupacao_id_cbo2002,
        procedimentos_realizados,
        procedimentos_realizados_anterior,
        procedimentos_por_hora,
        procedimentos_por_hora_anterior,
        round(
            100 * procedimentos_por_hora::numeric
            / nullif(procedimentos_por_hora_anterior, 0),
            1
        ) - 100 AS dif_procedimentos_por_hora_anterior,
        (
            coalesce(procedimentos_realizados, 0)
            - coalesce(procedimentos_realizados_anterior, 0)
        ) AS dif_procedimentos_realizados_anterior,
        now() AS atualizacao_data
    FROM ate_ultima_competencia resumo
)
SELECT * FROM final
