{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
caps_procedimentos AS (
    SELECT * FROM {{ ref('caps_procedimentos_individualizaveis') }}
),
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "estabelecimento_id_scnes",
            "usuario_id_cns_criptografado"
        ]) }} AS id,
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado,
        min(periodo_data_inicio) FILTER (
            WHERE procedimento_id_sigtap IN (
                '0301080232',  -- ACOLHIMENTO INICIAL POR CAPS
                '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (AC DEMANDA ESPONT)
            )
         ) AS acolhimento_periodo_data_inicio,
        min(periodo_data_inicio) FILTER (
            WHERE procedimento_id_sigtap NOT IN (
                '0301080232',  -- ACOLHIMENTO INICIAL POR CAPS
                '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (AC DEMANDA ESPONT)
            )
         ) AS primeiro_procedimento_periodo_data_inicio,
        now() AS atualizado_em
    FROM caps_procedimentos
    GROUP BY
        estabelecimento_id_scnes,
        usuario_id_cns_criptografado
)
SELECT * FROM final
