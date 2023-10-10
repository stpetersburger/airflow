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
from datetime import datetime as dt
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id="F_weekly_omniyat_scrapping",
    start_date=dt(2023, 10, 9),
    schedule_interval='00 14 * * *',
    catchup=False,
    tags=["prod"],
)

t1 = BashOperator(
    task_id="dld_scrap",
    bash_command=f"""python {os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/pipelines/scrap_dld2dwh.py """
                 f"""-conn gcp_omniyat -business_type dld -schema scrapers  
                 -date_from {str(dt.today().strftime('%m/%d/%Y'))} 
                 -date_to {str(dt.today().strftime('%m/%d/%Y'))}""",
    dag=dag
)

t1