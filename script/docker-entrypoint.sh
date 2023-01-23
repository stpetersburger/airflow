#!/usr/bin/env bash

# User-provided configuration must always be respected.
#
# Therefore, this old_script must only derives Airflow AIRFLOW__ variables from other variables
# when the user did not provide their own configuration.

TRY_LOOP="20"

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for - $host:$port... $j/$TRY_LOOP"
    sleep 5
  done
}

# Global defaults and back-compat
: "${AIRFLOW__CORE__EXECUTOR:=${AIRFLOW_EXECUTOR:-Sequential}Executor}"
if [ -z "$POSTGRES_EXTRAS" ]; then
#  : "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
  : "${AIRFLOW__CORE__FERNET_KEY:=${AIRFLOW__CORE__FERNET_KEY}}"
  export AIRFLOW__CORE__FERNET_KEY

  : "${AIRFLOW__WEBSERVER__SECRET_KEY:=${AIRFLOW__CORE__FERNET_KEY}}"
  : "${AIRFLOW__WEBSERVER__SECRET_KEY_CMD:=${AIRFLOW__CORE__FERNET_KEY}}"
  : "${AIRFLOW__WEBSERVER__SECRET_KEY_SECRET:=${AIRFLOW__CORE__FERNET_KEY}}"

  export \
    AIRFLOW__WEBSERVER__SECRET_KEY \
    AIRFLOW__WEBSERVER__SECRET_KEY_CMD \
    AIRFLOW__WEBSERVER__SECRET_KEY_SECRET
fi

#if a task forced finished from web UI then this amount of time has to pass before the automatic new restart
if [[ -z "${AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME}" ]]; then
  : "${AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME:=${AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME}}"
  export AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME
fi

#Load DAGs examples (default: Yes)
echo "${AIRFLOW__CORE__LOAD_EXAMPLES}"
if [[ "${AIRFLOW__CORE__LOAD_EXAMPLES:=1}" == 1 ]]; then
  AIRFLOW__CORE__LOAD_EXAMPLES=True
else
  AIRFLOW__CORE__LOAD_EXAMPLES=False
fi
#Expose airflow.cfg
if [[ "${AIRFLOW__CORE__EXPOSE_CONFIG:=1}" == 1 ]]; then
  AIRFLOW__CORE__EXPOSE_CONFIG=True
else
  AIRFLOW__CORE__EXPOSE_CONFIG=False
fi
#Do not execute new dags on creation
if [[ "${AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION:=1}" == 1 ]]; then
  AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
else
  AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
fi
# Check if the user has provided explicit Airflow configuration concerning the database
if [ -z "$AIRFLOW__CORE__SQL_ALCHEMY_CONN" ]; then
# Default values corresponding to the default compose files
#  : "${POSTGRES_HOST:=${POSTGRES_HOST}}"
#  : "${POSTGRES_PORT:=${POSTGRES_PORT}}"
#  : "${POSTGRES_USER:=${POSTGRES_USER}}"
#  : "${POSTGRES_PASSWORD:=${POSTGRES_PASSWORD}}"
#  : "${POSTGRES_DB:=${POSTGRES_DB}}"
#  : "${POSTGRES_EXTRAS:=${POSTGRES_EXTRAS:-""}}"

  if [ -z "$POSTGRES_EXTRAS" ]; then
    echo "NO POSTGRES EXTRAS"
  AIRFLOW__CORE__SQL_ALCHEMY_CONN="${POSTGRES_DRIVER}://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
  AIRFLOW__CELERY__RESULT_BACKEND="${CELERY_POSTGRES_DRIVER}://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
  else
  AIRFLOW__CORE__SQL_ALCHEMY_CONN="${POSTGRES_DRIVER}://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
  AIRFLOW__CELERY__RESULT_BACKEND="${CELERY_POSTGRES_DRIVER}://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
  fi
fi

export \
  AIRFLOW_HOME \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  AIRFLOW__CORE__EXPOSE_CONFIG \
  AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION \
  AIRFLOW__CORE__SQL_ALCHEMY_CONN \
  AIRFLOW__CELERY__RESULT_BACKEND

echo $AIRFLOW__CORE__SQL_ALCHEMY_CONN
echo $AIRFLOW__CORE__FERNET_KEY
echo $AIRFLOW__CELERY__RESULT_BACKEND
# Install custom python package if requirements.txt is present
#if [ -e "/requirements.txt" ]; then
#    $(command -v pip) install --user -r /requirements.txt
#fi

# Other executors than SequentialExecutor drive the need for an SQL database, here PostgreSQL is used

if [ "$AIRFLOW__CORE__EXECUTOR" != "SequentialExecutor" ]; then
  if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
      # Check if the user has provided explicit Airflow configuration concerning the broker
    if [ -z "$AIRFLOW__CELERY__BROKER_URL" ]; then
      # When Redis is secured by basic auth, it does not handle the username part of basic auth, only a token
      if [ -n "$REDIS_PASSWORD" ]; then
        REDIS_PREFIX=":${REDIS_PASSWORD}@"
        AIRFLOW__CELERY__BROKER_URL="${REDIS_PROTO}://:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}/${REDIS_DBNUM}"
      else
        AIRFLOW__CELERY__BROKER_URL="${REDIS_PROTO}://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DBNUM}"
      fi

      export AIRFLOW__CELERY__BROKER_URL
      echo $AIRFLOW__CELERY__BROKER_URL
    else
      # Derive useful variables from the AIRFLOW__ variables provided explicitly by the user
      REDIS_ENDPOINT=$(echo -n "$AIRFLOW__CELERY__BROKER_URL" | cut -d '/' -f3 | sed -e 's,.*@,,')
      REDIS_HOST=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f1)
      REDIS_PORT=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f2)
    fi
  fi
else

  # Derive useful variables from the AIRFLOW__ variables provided explicitly by the user
  POSTGRES_ENDPOINT=$(echo -n "$AIRFLOW__CORE__SQL_ALCHEMY_CONN" | cut -d '/' -f3 | sed -e 's,.*@,,')
  POSTGRES_HOST=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f1)
  POSTGRES_PORT=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f2)

fi

echo $AIRFLOW__CORE__EXECUTOR
echo $AIRFLOW__CORE__LOAD_EXAMPLES

echo "$@"

case "$@" in
  redis-server)
    echo "starting Redis"
    sleep 15
    if [ -n "$REDIS_PASSWORD" ]; then
      exec redis-server --port ${REDIS_PORT}
    else
      exec redis-server --port ${REDIS_PORT} --password ${REDIS_PASSWORD}
    fi
    echo "Redis started"

    echo "Waiting for port Celery"
    wait_for_port "${REDIS_HOST}" "${REDIS_HOST}" "${REDIS_PORT}"
    ;;
  webserver)
    echo "Waiting for port Postgres for Webserver"
    wait_for_port "${POSTGRES_HOST}" "${POSTGRES_HOST}" "${POSTGRES_PORT}"
    sleep 15
    echo "initiating db"
    airflow db init
    echo "db initiation finished"
    sleep 15
    airflow db upgrade
    echo "user creation start"
    airflow users create -u "${_AIRFLOW_WWW_USER_USERNAME}" \
                         -f "${_AIRFLOW_WWW_USER_USERNAME}" \
                         -l "${_AIRFLOW_WWW_USER_USERNAME}" \
                         -r "${_AIRFLOW_WWW_USER_ROLE}" \
                         -e "${_AIRFLOW_WWW_USER_EMAIL}" \
                         -p "${_AIRFLOW_WWW_USER_PASSWORD}"
    echo "user creation finished"
    echo "going to initialise webserver"

    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
      # With the "Local" and "Sequential" executors it should all run in one container.
      airflow scheduler &
    fi

    exec airflow "$@"
    ;;
  worker)
    echo "Starting Worker"
    # Give the webserver time to run db init.
    #sleep 10
    exec airflow celery "$@"
    ;;
  scheduler)
    echo "Starting Scheduler"
    # Give the webserver time to run db init.
    sleep 20
    exec airflow "$@"
    ;;
  flower)
    echo "Starting Flower"
    #sleep 10
    exec airflow celery "$@"
    ;;
  version)
    exec airflow celery "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac