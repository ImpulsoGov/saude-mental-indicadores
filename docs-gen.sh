#!/bin/bash

# SPDX-FileCopyrightText: 2022 Impulso Gov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


dbt docs generate && python tasks/docs-consolidate.py
