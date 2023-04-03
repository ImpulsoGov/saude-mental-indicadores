{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
aih AS (
    SELECT * FROM {{ ref("aih_rd_disseminacao_municipios_selecionados") }}
),
sexos AS (
    SELECT * FROM {{ source("codigos", "sexos") }}
),
unidades_geograficas AS (
    SELECT * FROM {{ source("codigos", "unidades_geograficas") }}
),
condicoes_saude_mental AS (
    SELECT * FROM {{ ref("condicoes_saude_mental") }}
),
aih_saude_mental_todas AS (
    SELECT
        -- utiliza unidade geográfica *de residência* do usuário como 
        -- referência, e não a do estabelecimento
        unidade_geografica.id AS unidade_geografica_id,
        unidade_geografica.id_sus AS unidade_geografica_id_sus,
        -- troca classificação própria de sexo do SIH pela do SIGTAP
        sexo.id_sigtap AS usuario_sexo_id_sigtap,
        condicao_saude_mental.classificacao AS condicao_saude_mental_classificao,
        {{ dbt_utils.star(
            from=ref("aih_rd_disseminacao_municipios_selecionados"),
            relation_alias="aih",
            except=[
                "id",
                "unidade_geografica_id",
                "unidade_geografica_id_sus",
                "usuario_sexo_id_sihsus",
            ]
        ) }}
    FROM aih
    -- Juntas as condições listadas como causas da internação às condições
    -- relativas a saúde mental
    LEFT JOIN condicoes_saude_mental condicao_saude_mental
    ON aih.condicao_principal_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    OR aih.condicao_secundaria_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    OR aih.condicao_secundaria_1_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    OR aih.condicao_secundaria_2_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    OR aih.condicao_secundaria_3_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    OR aih.condicao_secundaria_4_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    OR aih.condicao_secundaria_5_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    OR aih.condicao_secundaria_6_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    OR aih.condicao_secundaria_7_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    OR aih.condicao_secundaria_8_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    OR aih.condicao_secundaria_9_id_cid10 LIKE (
        condicao_saude_mental.id_cid10 || '%'
    )
    -- Atribui códigos padronizados da SIGTAP às categorias de sexo do usuário,
    -- já que o SIHSUS têm uma classificação própria
    LEFT JOIN sexos sexo
    ON aih.usuario_sexo_id_sihsus = sexo.id_sihsus
    -- utiliza unidade geográfica *de residência* do usuário como 
    -- referência, e não a do estabelecimento
    LEFT JOIN unidades_geograficas unidade_geografica
    ON aih.usuario_residencia_municipio_id_sus = unidade_geografica.id_sus
    WHERE (
        -- o leito é de especialidade de saúde mental
            aih.leito_especialidade_id_sigtap = ANY (ARRAY[
                '05',  -- Psiquiatria
                '14',  -- Leito dia/saude mental
                '84',  -- Acolhimento Noturno
                '87'  -- Saude Mental (clinico)
            ]::bpchar(2)[])
        -- OU o procedimento principal é diária em saúde mental
        OR aih.procedimento_solicitado_id_sigtap = ANY (ARRAY[
            -- DIARIA DE SAÚDE MENTAL COM PERMANENCIA DE ATÉ SETE DIAS
            '0802010253',
            -- DIÁRIA DE SAUDE MENTAL COM PERMANENCIA ENTRE 08 A 15 DIAS
            '0802010261',
            -- DIÁRIA DE SAUDE MENTAL COM PERMENENCIA SUPERIOR A 15 DIAS
            '0802010270'  
        ]::bpchar(10)[])
        -- OU o desfecho foi classificado como `'alta de paciente agudo em
        -- psiquiatria'`
        OR aih.desfecho_motivo_id_sihsus = '19'
        -- OU algum dos CIDs do diagnóstico é de saúde mental
        OR condicao_saude_mental.id_cid10 IS NOT NULL
    )
    {%- if is_incremental() %}
    AND aih.atualizacao_data > (
        SELECT max(atualizacao_data)
        FROM {{ this }}
    )
    {%- endif %}
),
-- Utiliza apenas a última remessa de AIH disponível para cada internação
final AS (
	SELECT
        -- TODO: considerar transferências (várias AIHs p/ a mesma internação)
		DISTINCT ON (aih_id_sihsus)
        {{ dbt_utils.surrogate_key(["aih_id_sihsus"]) }} AS id,
		*
    {%- if is_incremental() %}
    FROM (
        SELECT * FROM {{ this }}
        UNION
        SELECT *
    {%- endif %}
    FROM aih_saude_mental_todas
    {%- if is_incremental() %}
    ) _
    {%- endif %}
	ORDER BY 
	   aih_id_sihsus,
	   periodo_data_inicio DESC,
       atualizacao_data DESC
)
SELECT * FROM final
