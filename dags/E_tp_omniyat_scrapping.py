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

"""Daily job run at the beginning of working hours (8:30 am Iraq, Saudi Arabia; 9:30 am UAE)"""

import os
from airflow import DAG
from datetime import datetime as dt, timedelta as td
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id="E_tp_omniyat_scrapping",
    start_date=dt(2023, 3, 16),
    schedule_interval='10 0 * * 4',
    catchup=False,
    tags=["prod"],
)

t1 = BashOperator(
    task_id="tp_scrap",
    bash_command=f"""python {os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/pipelines/scrap_tp2dwh.py """
                 f"""-conn gcp_omniyat -business_type tp -schema scrapers""",
    retries=3,
    retry_delay=td(minutes=3),
    dag=dag
)

t2 = BashOperator(
    task_id="tp_analytics",
    bash_command=f"""python {os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/pipelines/externalfiles2dwh.py """
                 f"""-conn gcp_omniyat -schedule_type omniyat_scrapping_tp""",
    dag=dag
)

t1>>t2