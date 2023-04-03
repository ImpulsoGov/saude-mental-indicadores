{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
matriciamentos_por_caps AS (
    SELECT * FROM {{ ref('matriciamentos_por_caps_ultimo_ano') }}
),
matriciamentos_por_municipio AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        date_part('year', max(competencia))::text AS ano,
        listas_de_codigos.nome_mes(max(competencia)) AS ate_mes,
        sum(quantidade_registrada) AS quantidade_registrada,
        count(DISTINCT estabelecimento) FILTER (
            WHERE media_mensal_para_meta <= 1
        ) AS estabelecimentos_na_meta,
        count(DISTINCT estabelecimento) FILTER (
            WHERE media_mensal_para_meta > 1
        ) AS estabelecimentos_fora_meta
    FROM matriciamentos_por_caps
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
				"unidade_geografica_id"
		]) }} AS id,
		*
    FROM matriciamentos_por_municipio
)
SELECT * FROM final