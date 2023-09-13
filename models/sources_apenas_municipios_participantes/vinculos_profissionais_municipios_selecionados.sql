
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
vinculos_profissionais AS (
	SELECT * FROM {{ source("scnes", "vinculos_profissionais") }}
),
final AS (
    SELECT 
        * 
    FROM vinculos_profissionais
    WHERE estabelecimento_municipio_id_sus IN 
        (
            '150140', -- Belém/PA
            '230190', -- Barbalha/CE
            '230440', -- Fortaleza/CE (para Impulsolandia)
            '231290', -- Sobral/CE
            '261160', -- Recife/PE
            '280030', -- Aracaju/SE
            '292740', -- Salvador/BA
            '315780', -- Santa Luzia/MG
            '320500', -- Serra/ES
            '320520', -- Vila Velha/ES  
            -- '330490', -- São Gonçalo/RJ
            -- '350950', -- Campinas/SP
            '351640', -- Franco da Rocha/SP
            '352590', -- Jundiaí/SP
            -- '410480', -- Cascavel/PR
            -- '410690', -- Curitiba/PR
            '431440', -- Pelotas/RS
            '431490', -- Porto Alegre/RS   
            '520140'  -- Aparecida de Goiânia/GO                    
        )
)
SELECT * FROM final
