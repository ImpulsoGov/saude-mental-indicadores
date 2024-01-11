{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}

-- depends_on: {{ ref('_ambulatorio_procedimentos_por_profissional_por_hora') }}

WITH
{{ preparar_uso_externo(
	relacao="_ambulatorio_procedimentos_por_profissional_por_hora",
	cte_resultado="final"
) }}

SELECT
	id,
	unidade_geografica_id,
	unidade_geografica_id_sus,
	periodo_id,
	competencia,
	procedimentos_realizados,
	atualizacao_data,
	-- Processo necessário de ser feito com a coluna 'profissional_nome' para anonimizar Impulsolândia
    CASE 
		WHEN unidade_geografica_id_sus = '230440' THEN 
            CONCAT(
                LEFT(profissional_nome, POSITION(' ' IN profissional_nome) + 1),
                REGEXP_REPLACE(
                    SUBSTRING(profissional_nome FROM POSITION(' ' IN profissional_nome) + 1),
                    '[A-Za-z]',
                    '*',
                    'g'
                )
            )		
        ELSE profissional_nome
    END AS profissional_nome,
	disponibilidade_mensal,
	procedimentos_por_hora,
	ocupacao,
	estabelecimento_linha_perfil,
	estabelecimento_linha_idade,
	estabelecimento,
	periodo,
	nome_mes,
	periodo_ordem
FROM final