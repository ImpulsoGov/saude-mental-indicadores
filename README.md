<!--
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
-->

# Impulso Saúde Mental | Engenharia de indicadores

Este repositório contém os modelos de transformação de dados para o cálculo dos
indicadores da plataforma Impulso Saúde Mental, no formato utilizado pela
biblioteca de _analytics engineering_ [`dbt`][].

_**Nota**: atualmente (julho/2022), parte das transformações de dados para
cálculo dos indicadores estão implementadas como scripts SQL no repositório
[@ImpulsoGov/bd][scripts legados]. Esses scripts serão gradualmente movidos
para este repositório e reimplementados como modelos do `dbt`._

[`dbt`]: https://getdbt.com/
[scripts legados]: https://github.com/ImpulsoGov/bd/tree/main/Scripts/saude_mental

## Instalação

### Antes da instalação

As ferramentas presentes neste repositório pressupõem que a máquina em questão
possui o [Docker][] instalado para a execução de contêineres. Rodar a aplicação
fora do contêiner Docker providenciado junto ao repositório pode levar a
resultados inesperados.

A aplicação também pode ser executada diretamente no GitHub, por meio dos
fluxos de trabalho do [GitHub Actions][] que já são fornecidas no repositório.
Nesse caso, você deve apenas
[criar um *fork*](https://github.com/ImpulsoGov/saude-mental-indicadores/fork),
e os fluxos passarão a ser executados da sua conta do GitHub - inclusive
**consumindo da sua [cota de minutos][GH Actions Billing] do GitHub Actions**.

Em ambos os casos, a aplicação espera que o seu sistema contenha informações
importantes para a conexão com a instância do banco de dados, na forma de
variáveis de ambiente. Veja as variáveis esperadas no arquivo
[`.env.exemplo`][].

Se estiver desenvolvendo localmente, você pode fornecer essas variáveis
renomeando o arquivo `.env.exemplo` para `.env`, trocando os valores fictícios
pelos valores verdadeiros e rodando o comando `docker run` com o argumento
`--env-file=".env"` (ver abaixo). Caso esteja utilizando o GitHub Actions,
providencie essas variáveis de ambiente como
[segredos do repositório][GH Action Secrets].

Mais informações sobre a criação de uma instância de banco
de dados com estrutura semelhante à da ImpulsoGov podem ser encontrados no
repositório [@ImpulsoGov/bd][].

[Docker]: https://docs.docker.com/get-docker/
[GitHub Actions]: https://github.com/features/actions
[GH Actions Billing]: https://docs.github.com/pt/billing/managing-billing-for-github-actions/about-billing-for-github-actions
[`.env.exemplo`]: ./.env.exemplo
[GH Action Secrets]: https://docs.github.com/pt/actions/security-guides/encrypted-secrets
[@ImpulsoGov/bd]: https://github.com/ImpulsoGov/bd

### Obtendo e construindo a imagem Docker

Para obter a última versão da imagem docker, clone o repositório e chame o
comando `docker build` a partir do diretório clonado:

```sh
$ git clone https://github.com/ImpulsoGov/saude-mental-indicadores.git
$ cd saude-mental-indicadores
$ docker build -t impulsogov/saude-mental-indicadores:latest .
```

## Utilização

A imagem fornecida suporta os comandos [`dbt seed`][], [`dbt run`][] e
[`dbt test`][]. Com a imagem docker construída localmente, basta invocá-los com
o comando `docker run`:

```sh
$ docker run dbt run \
  --env-file=".env"  # se estiver desenvolvendo localmente
```

[`dbt seed`]: https://docs.getdbt.com/reference/commands/seed
[`dbt run`]: https://docs.getdbt.com/reference/commands/run
[`dbt test`]: https://docs.getdbt.com/reference/commands/test

No caso da utilização com GitHub Actions, a execução desses comandos é
automatizada com os fluxos de trabalho existentes, mas você também pode
iniciá-los manualmente na aba `Actions` do seu *fork*.
