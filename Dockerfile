# SPDX-FileCopyrightText: 2022 Impulso Gov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


FROM python:3.8.8-slim-buster as base

# Atualizar sistema
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
    curl \
    python-pip \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# Variáveis de ambiente
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Atualizar Python
RUN python -m pip install --upgrade setuptools wheel --no-cache-dir

# Criar diretório de trabalho
WORKDIR /usr/app/dbt/
VOLUME /usr/app/dbt/target
VOLUME /usr/app/dbt/logs

# Instalar poetry
FROM base as poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

# Instalar dependências
FROM poetry as dependencias-python
COPY ./pyproject.toml ./pyproject.toml
COPY ./poetry.lock ./poetry.lock
RUN poetry install --no-root --no-dev

# Instalar pacotes dbt
FROM dependencias-python as dependencias-dbt
ENV DBT_PROFILES_DIR="/usr/app/dbt/"
COPY ./dbt_project.yml ./dbt_project.yml
COPY ./packages.yml ./packages.yml
RUN poetry run dbt deps

# Copiar definições de modelos, macros, seeds etc.
FROM dependencias-dbt as final
COPY . .

# Usar `poetry run` em todos os comandos. 
ENTRYPOINT ["poetry", "run"]
