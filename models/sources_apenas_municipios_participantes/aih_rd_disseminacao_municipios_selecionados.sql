
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
aih_rd_disseminacao AS (
	SELECT * FROM {{ source("sihsus", "aih_rd_disseminacao") }}
),
final AS (
    SELECT 
        * 
    FROM aih_rd_disseminacao
    WHERE unidade_geografica_id_sus IN 
        (
            '230190', -- Barbalha/CE
            '231290', -- Sobral/CE
            '315780', -- Santa Luzia/MG
            '320500', -- Serra/ES
            '330490', -- São Gonçalo/RJ
            '350950', -- Campinas/SP
            '351640', -- Franco da Rocha/SP
            '410480', -- Cascavel/PR
            '520140', -- Aparecida de Goiânia/GO
            '150140', -- Belém/PA
            '261160', -- Recife/PE
            '280030', -- Aracaju/SE
            '292740', -- Salvador/BA
            '320530', -- Vitória/ES
            '410690', -- Curitiba/PR
            '431440', -- Pelotas/RS
            '431490' -- Porto Alegre/RS            
        )
)
SELECT * FROM final
