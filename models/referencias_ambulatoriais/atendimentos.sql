{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
bpa_i AS (
    SELECT *
    FROM {{ source('siasus', 'bpa_i_disseminacao') }}
    {% if is_incremental() -%}
    WHERE atualizacao_data > (
        SELECT max(atualizacao_data) FROM {{ this }}
    )
    {%- endif %}
),
estabelecimentos_referencia AS (
    SELECT * FROM {{ ref('estabelecimentos_referencia_ambulatorial') }}
),
bpa_i_referencias_ambulatoriais AS (
    SELECT
        {{ limpar_datas_atualizacao(source('siasus', 'bpa_i_disseminacao')) }},
        now() AS atualizacao_data
    FROM bpa_i
    INNER JOIN estabelecimentos_referencia
    USING (estabelecimento_id_cnes)
    WHERE bpa_i.profissional_ocupacao_id_cbo IN (
        '251510',  -- psicólogos
        '225133'  -- psiquiatras
    )
),
{{ classificar_faixa_etaria(
    relacao="bpa_i_referencias_ambulatoriais",
    coluna_data_nascimento="usuario_data_nascimento",
    coluna_data_referencia="realizacao_periodo_data_inicio",
    cte_resultado="final"
)}}
SELECT * FROM final
