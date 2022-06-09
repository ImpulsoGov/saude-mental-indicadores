{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
bpa_i AS (
    SELECT * FROM {{ source('siasus', 'bpa_i_disseminacao') }}
),
estabelecimentos_referencia AS (
    SELECT * FROM {{ ref('estabelecimentos_referencia_ambulatorial') }}
)
SELECT
    bpa_i.*,
    {{ classificar_faixa_etaria(
        "bpa_i.usuario_data_nascimento",
        "bpa_i.realizacao_periodo_data_inicio",
        [10, 20, 30, 40, 50, 60]
    ) }} AS usuario_faixa_etaria
FROM bpa_i
INNER JOIN estabelecimentos_referencia
ON
    bpa_i.estabelecimento_id_cnes
    = estabelecimentos_referencia.estabelecimento_id_cnes
WHERE bpa_i.profissional_ocupacao_id_cbo IN (
    '251510',  -- psicólogos
    '225133'  -- psiquiatras
)
