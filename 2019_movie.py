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

def gen_emp(id, rule='one_success'):
	op = EmptyOperator(task_id=id, trigger_rule=rule)
	return op

with DAG(
    '2019_movies',
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
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['api', 'movies'],
) as dag:
	#git branch바꿔야함!!!!
	REQUIREMENTS=["git+https://github.com/5Zigo-Gri-jo/load.git@d1.0.0/tmp_load",
	"git+https://github.com/5Zigo-Gri-jo/Extract.git@d2.0.0/temp_extract"]


	def get_parq(**kwargs):
		from datetime import datetime, timedelta
		from extract.ext import date_string, save2df 
		date_start = kwargs['date_start']
		date_lim = kwargs['date_lim']
		date_str = date_string(date_start)
		while date_str != date_lim:
			date_start = date_start + timedelta(days=1)
			date_str = date_string(date_start)
			df = save2df(date_str)

	#icebreaking를 임포트해서 리턴해주는 함수
	def ice_cat():
		from load.load import ice_breaking
		return ice_breaking()

	#Python Operator_Transform
	def tra_pvo(**kwargs):
		id = kwargs['id']
		op_kw = kwargs['op_kwargs']
		func_obj = kwargs['func_obj']
		task = PythonVirtualenvOperator(
			task_id=id, 
			python_callable=func_obj, 
			system_site_packages=False,
			requirements=REQUIREMENTS, 
			op_kwargs=op_kw
		)
		return task

	#Python Operator_Extract
	def ext_pvo(**kwargs):
		id = kwargs['id']
		#op_kw = kwargs['op_kwargs']
		func_obj = kwargs['func_obj']
		task = PythonVirtualenvOperator(
			task_id=id,
			python_callable=func_obj,
			system_site_packages=False,
			requirements=REQUIREMENTS,
			# op_kwargs=op_kw
		)
		
#Python Operator_Load

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
		if os.path.exists(path):
			return 'rm.dir'
		else:
			return 'get.data'
	def save_data(ds_nodash):
		from mov.api.call import apply_type2df
		df = apply_type2df(load_dt=ds_nodash)
		print(df.head(10))
		print(df.dtypes)
		
		g = df.groupby('openDt')
		sum_df = g.agg({'audiCnt' : 'sum'}).reset_index()
		print(df)

#Task
	task_e1 = PythonVirtualenvOperator(
		task_id='extract1',
		requirements=REQUIREMENTS,
		system_site_packages=False,
		python_callable=get_parq,
		op_kwargs = {'date_start':datetime(2018,12,31), 'date_lim':'20190530'}
		)

	task_e2 = PythonVirtualenvOperator(
		task_id='extract2',
		requirements=REQUIREMENTS,
		system_site_packages=False,
		python_callable=get_parq,
		op_kwargs = {'date_start':datetime(2019,5,30), 'date_lim':'20191031'}
		)

	task_e3 = PythonVirtualenvOperator(
		task_id='extract3',
		requirements=REQUIREMENTS,
		system_site_packages=False,
		python_callable=get_parq,
		op_kwargs = {'date_start':datetime(2019,10,31), 'date_lim':'20191231'}
		)

	task_t = PythonVirtualenvOperator(
		task_id='transform',
		requirements=REQUIREMENTS,
		system_site_packages=False,
		python_callable=ice_cat
        	)
	task_l = PythonVirtualenvOperator(
		task_id='load',
		requirements=REQUIREMENTS,
		system_site_packages=False,
		python_callable=ice_cat
		)
	task_save = PythonVirtualenvOperator(
		task_id='save.data',
		python_callable=save_data,
		system_site_packages=False,
		requirements=['git+https://github.com/thephunkmonk/movie_dag.git@0.2/api'],
		trigger_rule="one_success"
	)

	task_rm_dir = BashOperator(
		task_id='rm.dir',
		bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}'
	)

	branch_op = BranchPythonOperator(
		task_id="branch.op",
		python_callable=branch_func
	)

	task_start = gen_emp('start')
	task_end = gen_emp('end','all_done')

	task_join = gen_emp('join')

#Task_extract

#Graph
task_start >> task_rm_dir >> task_join >> task_e1 >> task_e2 >> task_e3 >> task_t >> task_l >> task_end
#branch_op >> task_get_start


#load >> task_save_data >> task_end
