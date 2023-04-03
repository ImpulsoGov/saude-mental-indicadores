{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}


WITH
bpa_i AS (
    SELECT *
    FROM {{ ref("bpa_i_disseminacao_municipios_selecionados") }}
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
    USING (estabelecimento_id_scnes)
    WHERE bpa_i.profissional_vinculo_ocupacao_id_cbo2002 IN (
        '251510',  -- psicólogos
        '225133'  -- psiquiatras
    )
),
{{ classificar_faixa_etaria(
    relacao="bpa_i_referencias_ambulatoriais",
    coluna_nascimento_data="usuario_nascimento_data",
    coluna_data_referencia="realizacao_periodo_data_inicio",
    cte_resultado="final"
)}}
SELECT * FROM final
