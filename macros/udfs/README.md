<!--
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
-->

# Funções do usuário

Esta pasta contém macros para definir [funções do usuário][UDF]
(*user-defined functions*, ou simplesmente UDFs).

Esses macros não devem ser utilizados diretamente, e sim invocados por meio da
operação [criar_udfs][] ([documentação][docs]) *antes* da execução das
transformações desejadas, de forma a garantir que as funções e procedimentos
esperados tenham sido registrados no banco de dados.

```sh
$ dbt run-operation criar_udfs  # deve ser chamado *antes* do `dbt run`!
$ dbt run
```

Para mais informações, consulte [esta discussão][topico-udfs] no fórum de
usuários do dbt.

[criar_udfs]: macros/criar_udfs
[docs]: https://impulsogov.github.io/saude-mental-indicadores/#!/macro/macro.impulso_saude_mental.criar_udfs
[UDF]: https://en.wikipedia.org/wiki/User-defined_function
[topico-udfs]: https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions/18
