
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
raas_psicossocial_disseminacao AS (
	SELECT * FROM {{ source("siasus", "raas_psicossocial_disseminacao") }}
),
final AS (
    SELECT 
        * 
    FROM raas_psicossocial_disseminacao
    WHERE unidade_geografica_id_sus IN 
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
            '520140',  -- Aparecida de Goiânia/GO     
            
            -- MUNICIPIOS BNDES
            -- '260120', -- Arcoverde/PE
            -- '210005', -- Açailândia/MA
            -- '150170', -- Bragança/PA
            -- '230280', -- Canindé/CE
            -- '280290', -- Itabaiana/SE
            -- '280350', -- Lagarto/SE
            -- '160030', -- Macapá/AP
            -- '280480', -- Nossa Senhora do Socorro/SE
            -- '230970', -- Pacatuba/CE
            '231130', -- Quixadá/CE
            -- '231140', -- Quixeramobim/CE
            '150680' -- Santarém/PA
            -- '261390', -- Serra Talhada/PE    
        )
)
SELECT * FROM final
