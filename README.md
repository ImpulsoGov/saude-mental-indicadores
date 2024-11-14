<!--
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
-->
![Badge DBT](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Badge em Desenvolvimento](https://img.shields.io/badge/status-em%20desenvolvimento-yellow)

# Impulso Saúde Mental | Engenharia de indicadores

Este repositório contém os modelos de transformação de dados para o cálculo dos
indicadores da plataforma Impulso Saúde Mental, no formato utilizado pela
biblioteca de _analytics engineering_ [`dbt`][].

_**Nota**: atualmente (dezembro/2022), parte das transformações de dados para
cálculo dos indicadores estão implementadas como scripts SQL no repositório
[@ImpulsoGov/bd][scripts legados]. Esses scripts serão gradualmente movidos
para este repositório e reimplementados como modelos do `dbt`._

[`dbt`]: https://getdbt.com/
[scripts legados]: https://github.com/ImpulsoGov/bd/tree/main/Scripts/saude_mental

*******
## :mag_right: Índice
1. [Contexto](#contexto)
2. [Estrutura do repositório](#estrutura)
4. [Instruções para instalação](#instalacao)
    1. [Antes da instalação](#preinstalacao)
    2. [Obtendo e construindo a imagem Docker](#imagemdocker)
5. [Utilização](#utilizacao)
6. [Contribua](#contribua)
7. [Licença](#licenca)
*******

<div id='contexto'/>  

## :rocket: Contexto

Um dos propósitos da ImpulsoGov, enquanto organização, é transformar dados da saúde pública do Brasil em informações que ofereçam oferecer suporte de decisão aos gestores de saúde pública em todo o Brasil. Embora o SUS tenha uma riqueza de dados há muitas dificuldades para reunir, coletar e analisar dados em diferentes sistemas de informação. O projeto de Saúde Mental tem como objetivo apoiar gestões da Rede de Atenção Psicossocial (RAPS) com uso de dados e indicadores desenvolvidos a partir do entendimento do dia a dia e dos principais desafios da rede. Hoje, este repositório estrutura a base de dados que alimenta uma ferramenta gratuita de visualização de indicadores disponibilizada aos gestores parceiros.
*******

<div id='estrutura'/>  
 
## :milky_way: Estrutura do repositório

O repositório é estruturado seguindo modelo necessário para utilização do DBT, 

```plain
├─ saude-mental-indicadores
│  ├─ analysis
│  ├─ docs
│  ├─ logs
│  ├─ macros
│  │  └─ ...
│  ├─ models
│  │  └─ codigos 
│  │  └─ sources
│  │  └─ ...
│  ├─ seeds
│  ├─ snapshots
│  ├─ target
│  ├─ tasks
│  ├─ tests
│  ├─ ...
└─ 
```

A pasta `macros` contém as funções de transformações de dados em Jinja, já a pasta `models` contém tanto os modelos em SQL responsáveis por criar as tabelas utilizadas no projeto, quanto as fontes contidas no banco de dados que alimentam as tabelas.
*******

<div id='instalacao'/> 

## 🛠️ Instalação

 <div id='preinstalacao'/> 
 
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
 

 <div id='imagemdocker'/>
 
 ### Obtendo e construindo a imagem Docker
 
 Para obter a última versão da imagem docker, clone o repositório e chame o
 comando `docker build` a partir do diretório clonado:
 
 ```sh
 $ git clone https://github.com/ImpulsoGov/saude-mental-indicadores.git
 $ cd saude-mental-indicadores
 $ docker build -t impulsogov/saude-mental-indicadores:latest .
 ```
 *******

<div id='utilizacao'/> 

## :gear: Utilização

A imagem fornecida suporta os comandos [`dbt seed`][], [`dbt run`][] e
[`dbt test`][]. _Com a imagem docker construída localmente_, basta invocá-los
com o comando `docker run`:

```sh
$ docker run -it\
> --env-file=".env" \
> --mount type=bind,source="$(pwd)/logs",destination="/usr/app/dbt/logs" \
> --mount type=bind,source="$(pwd)/target",destination="/usr/app/dbt/target" \
> impulsogov/saude-mental-indicadores:latest dbt run \
> --target analitico
```
Obs: 
- O alvo target aceita os valores 'analitico' ou 'producao'
- Ao atualizar completamente a base, também faz-se uso de --full-refresh após a indicação do target.


No caso da utilização com GitHub Actions, a execução desses comandos é
automatizada com os fluxos de trabalho existentes, mas você também pode
iniciá-los manualmente na aba `Actions` do seu *fork*.

[`dbt seed`]: https://docs.getdbt.com/reference/commands/seed
[`dbt run`]: https://docs.getdbt.com/reference/commands/run
[`dbt test`]: https://docs.getdbt.com/reference/commands/test
*******

<div id='contribua'/>  

## :left_speech_bubble: Contribua
Sinta-se à vontade para contribuir em nosso projeto! Abra uma issue ou envie PRs.

*******
<div id='licenca'/>  

## :registered: Licença
MIT © (?)
