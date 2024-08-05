### 2019 movie.py

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
	PythonOperator,
	PythonVirtualenvOperator,
	BranchPythonOperator
)

def gen_emp(id, rule='all_success'):
	op = EmptyOperator(task_id=id, trigger_rule=rule)
	return op

with DAG(
    '2019movie',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='2019 movie DAG',
    schedule="10 4 * * * ",
    start_date=datetime(2019, 1, 1),
    catchup=True,
    tags=['api', 'movies'],
) as dag:
	#git branch바꿔야함!!!!
	REQUIREMENTS=[
    "git+https://github.com/5Zigo-Gri-jo/load.git@d1.0.0/tmp_load",
	"git+https://github.com/5Zigo-Gri-jo/Extract.git@dates/d2.0.0"
    ]

	def looper():
		from datetime import datetime, timedelta
		from extract.ext import date_string
		date = datetime(2019,1,1)
		date_str = date_string(date)
		print("*" * 40)
		print("date_str:" + date_str)
		
        while date_str != '20191231':
			date = date - timedelta(days=1)
			date_str = date_string(date)
		    
            df = save2df(date_str)
            # df_all.append(df)
		print("looper")

	def get_data(ds_nodash):
		print(ds_nodash)
		from mov.api.call import gen_url, req, get_key, req2list, list2df, save2df
		key = get_key()
		print(f"movie api key => {key}")

		df = save2df(ds_nodash)

	def branch_func(ds_nodash):
		import os
		home_dir = os.path.expanduser("~")
		path = f'{home_dir}/data/2019movie/load_dt={ds_nodash}'
		if os.path.exists(path):
			return 'rm.dir'
		else:
			return 'get.data'

#Task
	task_e = PythonVirtualenvOperator(
		task_id='extract',
		requirements=REQUIREMENTS,
		system_site_packages=True,
		python_callable=looper
		)

	task_rm_dir = BashOperator(
		task_id='rm.dir',
		bash_command='rm -rf ~/data/2019movie/load_dt={{ ds_nodash }}'
	)

	branch_op = BranchPythonOperator(
        task_id="branch.op",
		python_callable=branch_func
	)

	task_start = gen_emp('start')
	task_end = gen_emp('end','all_done')

    task_start >> branch_op 
    task_branch >> task_rm_dir >> task_e
    task_branch >> task_e

    task_e >> task_t >> task_l >> task_end
