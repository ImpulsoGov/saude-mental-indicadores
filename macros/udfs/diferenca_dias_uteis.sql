{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro diferenca_dias_uteis() -%}
CREATE OR REPLACE FUNCTION {{ schema }}.diferenca_dias_uteis(
    data_inicio date,
    data_fim date
)
RETURNS integer
LANGUAGE plpgsql
AS $function$
	DECLARE
		data date;
		i int;
	BEGIN
		data := data_inicio;
		i := 0;
		-- TODO: pular feriados!
		WHILE (data < data_fim) LOOP
			data := {{ schema }}.proximo_dia_util(data);
			i := i + 1;
		END LOOP;
		RETURN i;
	END;
$function$
;
COMMENT ON FUNCTION {{ schema }}.diferenca_dias_uteis IS
'Função do usuário ([UDF][]) que calcula a diferença entre um par de datas
como o número de dias úteis no intervalo.
      
Atualmente, considera como dias úteis todos os dias de segunda a 
sexta-feira (feriados ainda não são levados em conta).

[UDF]: https://en.wikipedia.org/wiki/User-defined_function'
{%- endmacro -%}
