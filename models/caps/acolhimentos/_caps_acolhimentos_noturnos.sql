{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
raas_psicossocial_disseminacao AS (
    SELECT * FROM {{ source('siasus', 'raas_psicossocial_disseminacao') }}
    {% if is_incremental() %}
    {# TODO: suporte completo a atualizações retroativas #}
    WHERE criacao_data > (SELECT max(atualizacao_data) FROM {{ this }})
    {% endif %}
),
acolhimentos_noturnos AS (
    SELECT
        *
    FROM raas_psicossocial_disseminacao
    WHERE procedimento_id_sigtap IN (
        '0301080020',  -- acolhimento noturno em CAPS transtornos
        '0301080186'   -- acolhimento noturno em CAPS AD
    )
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
            "periodo_id"
        ]) }} AS id,
        unidade_geografica_id,
	    unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        count(DISTINCT usuario_id_cns_criptografado) AS acolhimentos_noturnos,
        sum(quantidade_apresentada) AS acolhimentos_noturnos_diarias,
        max(atualizacao_data) AS atualizacao_data
    FROM acolhimentos_noturnos
    GROUP BY
        unidade_geografica_id,
	    unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio
)
SELECT * FROM final
