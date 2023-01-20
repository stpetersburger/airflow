x# VERSION 1
# AUTHOR: "stpetersburger based on puckel"
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm --no-cache -t stpetersburger/airflow .
# SOURCE: https://github.com/stpetersburger/airflow

#base image
FROM python:3.8.12-slim-buster
LABEL maintainer="amalanalytics_"

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Build dependencies
ENV BUILD_DEPS 'freetds-dev \
libkrb5-dev \
libsasl2-dev \
libssl-dev \
libffi-dev \
libpq-dev \
git '

ENV BUILD_DEPS2 'freetds-bin \
build-essential \
default-libmysqlclient-dev \
apt-utils \
curl \
rsync \
netcat \
locales \
vim '

ENV BUILD_DEPS=${BUILD_DEPS}
# Airflow
ARG AIRFLOW_VERSION=2.2.4
ARG AIRFLOW_USER_HOME="/usr/local/airflow"
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

#RUN set -ex && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow
#RUN chown -R airflow: ${AIRFLOW_USER_HOME}
#RUN chgrp -R 0 ${AIRFLOW_USER_HOME} && chmod -R g+rwX ${AIRFLOW_USER_HOME}
#adding the user to root group (id=0) if needed
#RUN set -ex && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow -g 0
###


#COPY --chown=airflow ./requirements.txt /requirements.txt
#COPY --chown=airflow old_script/docker-entrypoint.sh /docker-entrypoint.sh
#COPY --chown=airflow old_configs/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
#COPY --chown=airflow ./dags ${AIRFLOW_USER_HOME}/dags
#COPY --chown=airflow ./logs ${AIRFLOW_USER_HOME}/logs
#COPY --chown=airflow ./plugins ${AIRFLOW_USER_HOME}/plugins

COPY ./requirements.txt /requirements.txt
COPY script/docker-entrypoint.sh /docker-entrypoint.sh

# Disable noisy "Handling signal" log messages:
# ENV GUNICORN_CMD_ARGS --log-level WARNING

RUN set -ex \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends $BUILD_DEPS $BUILD_DEPS2\
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && pip3 install -U -r ./requirements.txt \
    && apt-get purge --auto-remove -yqq $BUILD_DEPS \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY configs/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
COPY ./data ${AIRFLOW_USER_HOME}/data
COPY ./dags ${AIRFLOW_USER_HOME}/dags
COPY ./logs ${AIRFLOW_USER_HOME}/logs
COPY ./plugins ${AIRFLOW_USER_HOME}/plugins
COPY ./pyprojects/datawarehouse/pipelines ${AIRFLOW_USER_HOME}/pyprojects/datawarehouse/pipelines

EXPOSE 8080 5555 6379 5432 8786 2375 8125 5555
RUN chmod 755 docker-entrypoint.sh
#for docker.sock
RUN chmod 666 /var/run/*

#USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/docker-entrypoint.sh"]
#CMD ["webserver"]