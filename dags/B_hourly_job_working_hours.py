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

"""Hourly job run within working hours of Iraq, Saudi Arabia or UAE"""

import os
from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id="B_hourly_job_working_hours",
    start_date=datetime(2023, 3, 16),
    schedule_interval='05 5-15/1 * * *',
    catchup=False,
    tags=["prod"],
)

t1 = BashOperator(
    task_id="externalfiles2dwh",
    bash_command=f'python {os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/pipelines/externalfiles2dwh.py '
                 f'-conn gcp_bq -schedule_type hourly',
    dag=dag
)
