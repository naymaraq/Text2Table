#!/bin/bash
export AIRFLOW_HOME=${PWD}/airflow_home
export PYTHONPATH="${PYTHONPATH}:${PWD}"
airflow scheduler
