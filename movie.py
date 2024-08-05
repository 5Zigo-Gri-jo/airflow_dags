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
from pprint import pprint

def gen_emp(id, rule='all_success'):
	op = EmptyOperator(task_id=id, trigger_rule=rule)
	return op

with DAG(
    'movie',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie DAG',
    schedule="10 4 * * * ",
#    schedule=timedelta(days=1),
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['api', 'movies'],
) as dag:

	def common_get_data(ds_nodash, url_param):
		from mov.api.call import save2df
		df = save2df(load_dt=ds_nodash, url_param=url_param)
		print(df[['movieCd', 'movieNm']].head(5))
		for k, v in url_param.items():
			df[k] = v
		p_cols = ['load_dt'] + list(url_param.keys())
		df.to_parquet('~/tmp/test_parquet', partition_cols=p_cols)	
	def get_data(ds_nodash):
		print(ds_nodash)
		from mov.api.call import gen_url, req, get_key, req2list, list2df, save2df
		key = get_key()
		print(f"movie api key => {key}")

		df = save2df(ds_nodash)
		print(df.head(5))


	def branch_func(ds_nodash):
		import os
		home_dir = os.path.expanduser("~")
		path = f'{home_dir}/tmp/test_parquet/load_dt={ds_nodash}'
	#	path = os.path.join(home_dir, f'tmp/test_parquet/load_dt{ds_nodash}')
		if os.path.exists(path):
			return 'rm.dir'
		else:
			return 'get.data', "echo.task"
	def save_data(ds_nodash):
		from mov.api.call import apply_type2df
		df = apply_type2df(load_dt=ds_nodash)
		print(df.head(10))
		print(df.dtypes)
		
		g = df.groupby('openDt')
		sum_df = g.agg({'audiCnt' : 'sum'}).reset_index()
		print(df)

	
	rm_dir = BashOperator(
		task_id='rm.dir',
		bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}'
	)

	branch_op = BranchPythonOperator(
		task_id="branch.op",
		python_callable=branch_func
	)

	run_this = PythonOperator(
		task_id="print_the_context",
		python_callable=print_context
	)

	task_start = gen_emp('start')
	task_end = gen_emp('end','all_done')
#EmptyOperator(task_id = 'task_end', trigger_rule="all_done")
	task_get = PythonVirtualenvOperator(
        	task_id="get.data",
		python_callable=get_data,
		requirements=['git+https://github.com/thephunkmonk/movie_dag.git@0.2/api'],
                system_site_packages=False,
		trigger_rule='all_done',
		#venv_cache_path="/Users/kobatochan/tmp/airflow_venv/get_data"
	)
	task_save = PythonVirtualenvOperator(
		task_id='save.data',
		python_callable=save_data,
		system_site_packages=False,
		#venv_cache_path="/Users/kobatochan/tmp/airflow_venv/get_data",
		requirements=['git+https://github.com/thephunkmonk/movie_dag.git@0.2/api'],
		trigger_rule="one_success"
   	)
   
	task_done = BashOperator(
        	task_id="make.done",
        	bash_command="""
			echo 'done'
	"""
	)
	task_err = BashOperator(
        	task_id="err.report",
        	bash_command="""
			echo "error"
		""",
        	trigger_rule="one_failed"
	) 


task_start >> branch_op
#branch_op >> task_fetch
branch_op >> echo_task >> task_fetch
branch_op >> rm_dir >> task_fetch

task_fetch >> [task_get, multi_y, multi_n, nation_k, nation_f] >> task_emptysave




task_start >> run_this >> task_end


