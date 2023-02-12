{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


WITH
caps_procedimentos AS (
    SELECT * FROM {{ ref('caps_procedimentos_individualizaveis') }}
),
condicoes_saude AS (
    SELECT 
        DISTINCT ON (id_cid10)
        *
    FROM {{ source("codigos", "condicoes_saude") }}
    ORDER BY id_cid10
),
usuarios AS (
	SELECT 
        DISTINCT ON (
	       estabelecimento_id_scnes,
	       usuario_id_cns_criptografado
	    )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,        
        estabelecimento_tipo_id_sigtap,        
        usuario_id_cns_criptografado,        
        usuario_nascimento_data,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        condicoes_saude.grupo_descricao_curta_cid10 AS usuario_condicao_saude,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias,
        carater_atendimento_id_siasus,
        procedimento_id_sigtap,
        quantidade_apresentada,
        atualizacao_data
	FROM caps_procedimentos    
    LEFT JOIN condicoes_saude
    ON caps_procedimentos.condicao_principal_id_cid10 = condicoes_saude.id_cid10
	WHERE 
        quantidade_apresentada > 0 AND 
        procedimento_id_sigtap NOT IN (
            '0301080232',  -- ACOLHIMENTO INICIAL POR CAPS
            '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (AC DEMANDA ESPONT)
            ) AND
        estabelecimento_tipo_id_sigtap = '70' -- CAPS
	ORDER BY 
       estabelecimento_id_scnes,
       usuario_id_cns_criptografado,
       periodo_data_inicio asc
),
{{ classificar_faixa_etaria(
    relacao="usuarios",
    coluna_nascimento_data="usuario_nascimento_data",
    coluna_data_referencia="periodo_data_inicio",
    idade_tipo="Anos",
    faixa_etaria_agrupamento="10 em 10 anos",
    colunas_faixa_etaria=["id", "descricao", "ordem"],
    cte_resultado="usuarios_com_idade"
) }},
_usuarios_novos AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        estabelecimento_id_scnes,
        periodo_data_inicio,
        usuario_condicao_saude,
        usuario_sexo_id_sigtap,
        usuario_abuso_substancias,
        usuario_situacao_rua,
        usuario_raca_cor_id_siasus,
        faixa_etaria_id AS usuario_faixa_etaria_id,
        faixa_etaria_descricao AS usuario_faixa_etaria,
        faixa_etaria_ordem AS usuario_faixa_etaria_ordem,
        extract(YEAR FROM age(periodo_data_inicio, usuario_nascimento_data))
            AS usuario_idade,
        count(DISTINCT usuario_id_cns_criptografado) AS usuarios_novos
    FROM usuarios_com_idade
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_scnes,
        usuario_condicao_saude,
        usuario_sexo_id_sigtap,
        usuario_abuso_substancias,
        usuario_situacao_rua,
        usuario_raca_cor_id_siasus,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria,
        usuario_faixa_etaria_ordem,
        usuario_idade
),
{{ juntar_periodos_consecutivos(
    relacao="_usuarios_novos",
    agrupar_por=[
        "unidade_geografica_id", 
        "unidade_geografica_id_sus", 
        "estabelecimento_id_scnes", 
        "usuario_condicao_saude", 
        "usuario_sexo_id_sigtap", 
        "usuario_abuso_substancias", 
        "usuario_situacao_rua", 
        "usuario_raca_cor_id_siasus", 
        "usuario_faixa_etaria_id", 
        "usuario_faixa_etaria", 
        "usuario_faixa_etaria_ordem", 
        "usuario_idade",
    ],
    colunas_valores=[
        "usuarios_novos"
    ],
    periodo_tipo="Mensal",
    coluna_periodo="periodo_id",    
    colunas_adicionais_periodo=[
        "periodo_data_inicio"
        ],
    cte_resultado="com_periodo_anterior"
) }},
{{ ultimas_competencias(
    relacao="com_periodo_anterior",
    fontes=["raas_psicossocial_disseminacao"],
    meses_antes_ultima_competencia=(0, none),
    cte_resultado="ate_ultima_competencia"
) }},
intermediaria AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,        
        estabelecimento_id_scnes,
        usuario_condicao_saude,
        usuario_sexo_id_sigtap,
        saude_mental.classificar_binarios(
            usuario_situacao_rua
        ) AS usuario_situacao_rua,
        saude_mental.classificar_binarios(
            usuario_abuso_substancias
        ) AS usuario_abuso_substancias,
        usuario_raca_cor_id_siasus,
        usuario_faixa_etaria_id,
        usuario_faixa_etaria,
        usuario_faixa_etaria_ordem,
        usuario_idade,
        usuarios_novos,
        usuarios_novos_anterior,
        now() AS atualizacao_data   
    FROM ate_ultima_competencia
),
{{ classificar_caps_linha(
    relacao="intermediaria",
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    coluna_estabelecimento_id="estabelecimento_id_scnes",
    todos_estabelecimentos_id=none,
    todas_linhas_valor=none,
    cte_resultado="por_estabelecimento_com_linhas_cuidado"
) }},
{{ calcular_subtotais(
    relacao="por_estabelecimento_com_linhas_cuidado",
    agrupar_por=[
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id",
        "periodo_data_inicio",        
        "usuario_faixa_etaria_id",
        "usuario_faixa_etaria",
        "usuario_faixa_etaria_ordem",
        "usuario_sexo_id_sigtap",
        "usuario_condicao_saude",
        "usuario_raca_cor_id_siasus",
        "usuario_situacao_rua",
        "usuario_abuso_substancias",
        "usuario_idade"
    ],
    colunas_a_totalizar=[
		"estabelecimento_linha_perfil",
    	"estabelecimento_linha_idade",
		"estabelecimento_id_scnes"
    ],
    nomes_categorias_com_totais=["Todos", "Todos", "0000000"],
    agregacoes_valores={
        "usuarios_novos": "sum",
        "usuarios_novos_anterior": "sum",
        "atualizacao_data": "max"
    },
    manter_original=true,
    cte_resultado="usuario_perfil_incluindo_totais"
) }},
final AS (
    SELECT
        {{ dbt_utils.surrogate_key([
            "unidade_geografica_id",
		    "estabelecimento_linha_perfil",
    	    "estabelecimento_linha_idade",
            "estabelecimento_id_scnes",
            "periodo_id",
            "usuario_faixa_etaria_id",
            "usuario_sexo_id_sigtap",
            "usuario_condicao_saude",
            "usuario_raca_cor_id_siasus",
            "usuario_situacao_rua",
            "usuario_abuso_substancias"
        ]) }} AS id,
        *,        
        (
            coalesce(usuario_perfil_incluindo_totais.usuarios_novos, 0)
            - coalesce(usuario_perfil_incluindo_totais.usuarios_novos_anterior, 0)
        ) AS dif_usuarios_novos_anterior
    FROM usuario_perfil_incluindo_totais
)
SELECT * FROM final