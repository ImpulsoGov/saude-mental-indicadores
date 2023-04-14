
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
            '261160', -- Recife/PE
            '280030', -- Aracaju/SE
            '431490', -- Porto Alegre/RS 
            '315780', -- Santa Luzia/MG
            '351640', -- Franco da Rocha/SP
            '230190', -- Barbalha/CE
            '231290', -- Sobral/CE            
            '320500', -- Serra/ES
            '330490', -- São Gonçalo/RJ
            '350950', -- Campinas/SP            
            '410480', -- Cascavel/PR
            '520140', -- Aparecida de Goiânia/GO            
            '292740', -- Salvador/BA
            '320530', -- Vitória/ES
            '431440', -- Pelotas/RS
            '410690' -- Curitiba/PR                       
        )
)
SELECT * FROM final
