{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{% macro 
    classificar_faixa_etaria(
        coluna_data_nascimento,
        coluna_data_referencia,
        limites_categorias
    )
%}
(CASE
    {% for limite in limites_categorias %}
    {% if loop.first %}
    WHEN age({{coluna_data_referencia}}, {{coluna_data_nascimento}}) < '{{ limite }} years'::interval THEN '0 a {{ limite }}'
    {% elif not loop.last %}
    WHEN age({{coluna_data_referencia}}, {{coluna_data_nascimento}}) < '{{ limite }} years'::interval THEN '{{ limite }} a {{ loop.nextitem }}'
    {% else %}
    ELSE '{{ limite }} ou mais'
    {% endif %}
    {% endfor %}
END)
{% endmacro %}
