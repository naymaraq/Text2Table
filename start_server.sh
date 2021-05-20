AIRFLOW="airflow_home"
export PYTHONPATH="${PYTHONPATH}:${PWD}"
export AIRFLOW_HOME=${PWD}/airflow_home
airflow version
airflow db init
echo "AUTH_ROLE_PUBLIC = 'Admin'" >> ${AIRFLOW}/webserver_config.py
airflow webserver --port 6969
