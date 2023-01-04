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
{%- set re = modules.re -%}
{%- set colunas = [] -%}
{%- for coluna in adapter.get_columns_in_relation(ref(relacao)) -%}
    {%- set _ = colunas.append(coluna.name) -%}
{%- endfor -%}
{%- set ctes = [ref(relacao)] -%}
{#- Trocar códigos de categorias de sexo por nomes legíveis -#}
{%- set colunas_sexo_ids = filtrar_regex(colunas, ".*sexo.*_id.*") -%}
{%- for coluna_sexo_id in colunas_sexo_ids %}
{%- set coluna_sexo_nome=(
    re.match("(.*sexo.*)_id.*", coluna_sexo_id).groups(1)[0]
) -%}
{%- set cte = "com_nomes_sexos_" + loop.index|string -%}
{{ nomear_sexos(
    relacao=ctes|last,
	coluna_sexo_nome=coluna_sexo_nome,
    coluna_sexo_id=coluna_sexo_id,
	todos_sexos_id=none,
    cte_resultado=cte
) }},
{%- set _ = colunas.append(coluna_sexo_nome) -%}
{%- set _ = ctes.append(cte) -%}
{%- endfor %}
{#- Trocar códigos de categorias de ocupação por nomes legíveis -#}
{%- set colunas_ocupacao_ids = filtrar_regex(colunas, ".*ocupacao.*_id.*") -%}
{%- for coluna_ocupacao_id in colunas_ocupacao_ids %}
{%- set coluna_ocupacao_nome=(
    re.match("(.*ocupacao.*)_id.*", coluna_ocupacao_id).groups(1)[0]
) -%}
{%- set cte = "com_nomes_ocupacoes_" + loop.index|string -%}
{{ nomear_ocupacoes(
    relacao=ctes|last,
	coluna_ocupacao_nome=coluna_ocupacao_nome,
    coluna_ocupacao_id=coluna_ocupacao_id,
	todas_ocupacoes_id="000000",
    cte_resultado=cte
) }},
{%- set _ = colunas.append(coluna_ocupacao_nome) -%}
{%- set _ = ctes.append(cte) -%}
{%- endfor %}
{#- Trocar códigos de categorias de raça/cor por nomes legíveis -#}
{%- set colunas_raca_cor_ids = filtrar_regex(colunas, ".*raca_cor.*_id.*") -%}
{%- for coluna_raca_cor_id in colunas_raca_cor_ids %}
{%- set coluna_raca_cor_nome=(
    re.match("(.*raca_cor.*)_id.*", coluna_raca_cor_id).groups(1)[0]
) -%}
{%- set cte = "com_nomes_racas_cores_" + loop.index|string -%}
{{ nomear_racas_cores(
    relacao=ctes|last,
	coluna_raca_cor_nome=coluna_raca_cor_nome,
    coluna_raca_cor_id=coluna_raca_cor_id,
	todas_racas_cores_id=none,
    cte_resultado=cte
) }},
{%- set _ = colunas.append(coluna_raca_cor_nome) -%}
{%- set _ = ctes.append(cte) -%}
{%- endfor %}
{#- Trocar códigos de estabelecimentos por nomes legíveis -#}
{%- set colunas_estabelecimento_ids = filtrar_regex(
    colunas,
    ".*estabelecimento.*_id(?:_.+)*$",
) -%}
{%- for coluna_estabelecimento_id in colunas_estabelecimento_ids -%}
{%- set coluna_estabelecimento_nome=(
    re.match("(.*estabelecimento.*)_id(?:_.+)*$", coluna_estabelecimento_id)
    .groups(1)[0]
) -%}
{%- set coluna_estabelecimento_linha_perfil = (
    coluna_estabelecimento_nome + "_linha_perfil"
) -%}
{%- set coluna_estabelecimento_linha_idade = (
    coluna_estabelecimento_nome + "_linha_idade"
) %}
{%- if (
        not coluna_estabelecimento_linha_perfil in colunas
    ) and (
        not coluna_estabelecimento_linha_idade in colunas
    )
%}
{%- set cte = "com_linhas_estabelecimentos_" + loop.index|string -%}
{{ classificar_caps_linha(
    relacao=ctes|last,
    coluna_estabelecimento_id=coluna_estabelecimento_id,
    coluna_linha_perfil=coluna_estabelecimento_linha_perfil,
    coluna_linha_idade=coluna_estabelecimento_linha_idade,
    cte_resultado=cte
) }},
{%- set _ = ctes.append(cte) -%}
{%- set _ = colunas.append(coluna_estabelecimento_linha_perfil) -%}
{%- set _ = colunas.append(coluna_estabelecimento_linha_idade) -%}
{%- endif %}
{%- set cte = "com_nomes_estabelecimentos_" + loop.index|string -%}
{{ nomear_estabelecimentos(
    relacao=ctes|last,
    coluna_estabelecimento_id=coluna_estabelecimento_id,
	coluna_estabelecimento_nome=coluna_estabelecimento_nome,
    cte_resultado=cte
) }},
{%- set _ = colunas.append(coluna_estabelecimento_nome) -%}
{%- set _ = ctes.append(cte) -%}
{%- endfor %}
{%- if (
        "periodo_data_inicio" in colunas
    ) and (
        "unidade_geografica_id" in colunas
    )
%}
{%- set cte = "com_datas_legiveis" -%}
{{ adicionar_datas_legiveis(
	relacao=ctes|last,
	cte_resultado=cte
) }},
{%- set _ = colunas.append("periodo") -%}
{%- set _ = colunas.append("nome_mes") -%}
{%- set _ = colunas.append("periodo_ordem") -%}
{%- set _ = ctes.append(cte) -%}
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
    FROM {{ ctes|last }}
)
{%- endmacro -%}
