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
COPY . .

# Instalar poetry
FROM base as poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

# Instalar dependências e plugins
FROM poetry as dependencias
RUN poetry install --no-root --no-dev
RUN poetry run dbt deps

# Inicializar dbt
FROM dependencias as dbt
ENV DBT_PROFILES_DIR="/usr/app/dbt/"

ENTRYPOINT ["poetry", "run"]
