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
from datetime import datetime
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id="D_daily_omniyat_ga",
    start_date=datetime(2023, 3, 16),
    schedule_interval='00 5 * * *',
    catchup=False,
    tags=["prod"],
)

t4 = BashOperator(
    task_id="externalfiles2dwh_com",
    bash_command=f'python {os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/pipelines/externalfiles2dwh.py '
                 f'-conn gcp_omniyat -schedule_type omniyatcom_ga_daily -schema gcp_ga_com',
    dag=dag
)

t3 = BashOperator(
    task_id="externalfiles2dwh",
    bash_command=f'python {os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/pipelines/externalfiles2dwh.py '
                 f'-conn gcp_omniyat -schedule_type omniyat_ga_daily -schema gcp_ga',
    dag=dag
)


t1 = BashOperator(
    task_id="ga",
    bash_command=f"""python {os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/pipelines/ga2dwh.py """
                 f"""-conn gcp_omniyat -business_type omniyat -schema gcp_ga""",
    dag=dag
)

t2 = BashOperator(
    task_id="ga_com",
    bash_command=f"""python {os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/pipelines/ga2dwh.py """
                 f"""-conn gcp_omniyat -business_type omniyatcom -schema gcp_ga_com""",
    dag=dag
)

t5 = BashOperator(
    task_id="facebook_ads",
    bash_command=f"""python {os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/pipelines/fb2dwh.py """
                 f"""-conn gcp_omniyat -schema facebook_ads""",
    dag=dag
)

[t1>>t3,t2>>t4,t5]
