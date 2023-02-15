#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the DockerOperator."""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import os
import sys

#pwds = Variable.get("AIRFLOW__BI__CONNPWDS")
#conns = Variable.get("AIRFLOW__BI__CONNS")
#customEnv = {
#'AIRFLOW__BI__CONNPWD': f"""'{pwds}'""",
#'AIRFLOW__BI__CONN': f"""'{conns}'"""
#}

#env = dict(os.environ, **customEnv)

dag = DAG(
    dag_id="externalfiles2dwh",
    start_date=datetime(2023, 2, 10),
    schedule_interval='45 2-12/3 * * *',
    catchup=False,
    tags=["staging"],
)

t1 = BashOperator(
    task_id="externalfiles2dwh",
    bash_command=f'python {os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/pipelines/externalfiles2dwh.py '
                 f'-conn gcp_bq',
 #   env=env,
    dag=dag
)

t1