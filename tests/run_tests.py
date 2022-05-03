# Databricks notebook source

# MAGIC %md Test runner for `pytest`

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import pytest
retcode = pytest.main(["--junitxml=/tmp/test-unit.xml", "-lr", "/Workspace/Repos/rafi.kurlansik@databricks.com/e2e-cuj"])



