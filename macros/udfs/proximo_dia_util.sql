{#
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
 
SPDX-License-Identifier: MIT
#}


{%- macro proximo_dia_util() -%}
CREATE OR REPLACE FUNCTION {{ schema }}.proximo_dia_util(data date)
RETURNS date
LANGUAGE plpgsql
AS $function$
	BEGIN
		data := data + '1 day'::interval;
		-- TODO: pular feriados!
		WHILE (EXTRACT(dow FROM data) = 0) OR (EXTRACT(dow FROM data) = 6) LOOP
			data := data + '1 day'::interval;
		END LOOP;
		RETURN data;
	END;
$function$
;
COMMENT ON FUNCTION {{ schema }}.proximo_dia_util IS 
'Função do usuário ([UDF][]) que, para uma data qualquer, obtém a próxima
data que seja um dia de semana.

Atualmente, considera como dias úteis todos os dias de segunda a
sexta-feira (feriados ainda não são levados em conta).

[UDF]: https://en.wikipedia.org/wiki/User-defined_function'
;
{%- endmacro -%}
