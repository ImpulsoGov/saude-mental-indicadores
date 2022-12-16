
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
ceps AS (
	SELECT * FROM {{ source("codigos", "ceps") }}
),
final AS (
    SELECT 
        unidade_geografica_id,
        municipio_id_sus,
        bairro_nome,
        array_agg(id_cep) AS ceps,
        min(latitude) + (max(latitude) - min(latitude))/2 AS latitude,
        min(longitude) + (max(longitude) - min(longitude))/2 AS longitude
    FROM ceps
    INNER JOIN listas_de_codigos.ceps_por_municipio
    USING (id_cep)
    GROUP BY unidade_geografica_id, municipio_id_sus, bairro_nome
)
SELECT * FROM final
