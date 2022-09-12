
{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
#}



{%- macro preparar_uso_externo(
    relacao,
    remover_ids = true,
    remover_ids_exceto = [
        "unidade_geografica_id",
        "unidade_geografica_id_sus",
        "periodo_id"
    ],
    sufixos_a_eliminar=[
        "_nome",
        "_descricao"
    ],
    cte_resultado="final"
) %}
{% set re = modules.re %}
{%- set colunas = [] -%}
{%- for coluna in adapter.get_columns_in_relation(ref(relacao)) -%}
    {%- set _ = colunas.append(coluna.name) -%}
{%- endfor -%}
{%- set ultima_cte = ref(relacao) -%}
{%- if "sexo_id_sigtap" in colunas %}
{{ nomear_sexos(
    relacao=ultima_cte,
	coluna_sexo_nome="sexo",
	todos_sexos_id=none,
    cte_resultado="com_nomes_sexos"
) }},
{%- set ultima_cte = "com_nomes_sexos" -%}
{%- endif %}
{%- if "raca_cor_id_sigtap" in colunas %}
{{ nomear_racas_cores(
    relacao=ultima_cte,
	coluna_raca_cor_nome="raca_cor",
	todos_racas_cores_id=none,
    cte_resultado="com_nomes_racas_cores"
) }},
{%- set ultima_cte = "com_nomes_racas_cores" -%}
{%- endif %}
{%- if "estabelecimento_id_scnes" in colunas %}
{{ classificar_caps_linha(
    relacao=ultima_cte,
    coluna_linha_perfil="estabelecimento_linha_perfil",
    coluna_linha_idade="estabelecimento_linha_idade",
    cte_resultado="com_linhas_cuidado"
) }},
{%- set _ = colunas.append("estabelecimento_linha_perfil") -%}
{%- set _ = colunas.append("estabelecimento_linha_idade") -%}
{%- set ultima_cte = "com_linhas_cuidado" -%}
{%- endif %}
{%- if "estabelecimento_id_scnes" in colunas %}
{{ nomear_estabelecimentos(
    relacao=ultima_cte,
	coluna_estabelecimento_nome="estabelecimento",
    cte_resultado="com_nomes_estabelecimentos"
) }},
{%- set _ = colunas.append("estabelecimento") -%}
{%- set ultima_cte = "com_nomes_estabelecimentos" -%}
{%- endif %}
{%- if (
        "periodo_data_inicio" in colunas
    ) and (
        "unidade_geografica_id" in colunas
    )
%}
{{ adicionar_datas_legiveis(
	relacao=ultima_cte,
	cte_resultado="com_datas_legiveis"
) }},
{%- set _ = colunas.append("periodo") -%}
{%- set _ = colunas.append("nome_mes") -%}
{%- set _ = colunas.append("periodo_ordem") -%}
{%- set ultima_cte = "com_datas_legiveis" -%}
{%- endif %}
{{ cte_resultado }} AS (
    SELECT
    {%- for coluna in colunas %}
    {%- if coluna == "periodo_data_inicio" %}
        periodo_data_inicio AS competencia{{ "," if not loop.last }}
    {%- else %}
    {%- if (
            (not remover_ids)
        or (coluna in remover_ids_exceto)
        or (not re.match(".*_id(_.*)?$", coluna))
    ) %}
        {{ re.sub(
            "(.*)" + (sufixos_a_eliminar|join("|")) + "$",
            "\1",
            coluna
        ) }}{{ "," if not loop.last }}
    {%- endif %}
    {%- endif %}
    {%- endfor %}
    FROM {{ ultima_cte }}
)
{%- endmacro -%}
