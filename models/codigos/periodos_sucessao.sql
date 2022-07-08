{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
periodos AS (
    SELECT * FROM {{ source('codigos', 'periodos') }}
),
final AS (
    SELECT p1.tipo AS periodo_tipo,
        p2.id AS ultimo_periodo_id,
        p1.id AS periodo_id,
        p3.id AS proximo_periodo_id,
        p2.data_inicio AS ultimo_periodo_data_inicio,
        p2.data_fim AS ultimo_periodo_data_fim,
        p1.data_inicio AS periodo_data_inicio,
        p1.data_fim AS periodo_data_fim,
        p3.data_inicio AS proximo_periodo_data_inicio,
        p3.data_fim AS proximo_periodo_data_fim
    FROM listas_de_codigos.periodos p1
    LEFT JOIN listas_de_codigos.periodos p2
        ON
            p1.tipo::text = p2.tipo::text 
        AND p1.data_inicio = (p2.data_fim + '1 day'::interval)
    LEFT JOIN listas_de_codigos.periodos p3
        ON
            p1.tipo::text = p3.tipo::text
        AND p3.data_inicio = (p1.data_fim + '1 day'::interval)
)
SELECT * FROM final
