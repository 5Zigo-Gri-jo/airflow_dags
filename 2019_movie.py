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
	REQ=["git+https://github.com/5Zigo-Gri-jo/load.git@d1.0.0/tmp_load",
	"git+https://github.com/5Zigo-Gri-jo/Extract.git@d2.0.0/temp_extract",
	"git+https://github.com/5Zigo-Gri-jo/transform.git@d3.0.0/apply_type"]


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
	def tra_pvo():
		from transform.trans import apply_type2df
		import pandas as pd
		from datetime import datetime,timedelta
		from extract.ext import date_string
		df_all = pd.DataFrame()
		date = datetime(2019,1,1)
		date_str = date_string(date)
		while date_str != '20200101':
			df = apply_type2df(date_str)
			df_all = pd.concat([df_all, df], ignore_index=True)
			date = date+timedelta(days=1)
			date_str = date_string(date)

		df_all.to_parquet('~/data/2019movie/tmp.parquet')
		return df_all

	def avgmonth():
		from transform.trans import avg_month
		return avg_month()
	#Python Operator_Extract
	def ext_pvo(**kwargs):
		id = kwargs['id']
		#op_kw = kwargs['op_kwargs']
		func_obj = kwargs['func_obj']
		task = PythonVirtualenvOperator(
			task_id=id,
			python_callable=func_obj,
			system_site_packages=False,
			requirements=REQ,
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
		path1 = f'{home_dir}/data/2019movie/20190530.parquet'
		path2 = f'{home_dir}/data/2019movie/20191031.parquet'
		path3 = f'{home_dir}/data/2019movie/20191231.parquet'
		if os.path.exists(path1):
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
		requirements=REQ,
		system_site_packages=False,
		python_callable=get_parq,
		op_kwargs = {'date_start':datetime(2018,12,31), 'date_lim':'20190530'}
		)

	task_e2 = PythonVirtualenvOperator(
		task_id='extract2',
		requirements=REQ,
		system_site_packages=False,
		python_callable=get_parq,
		op_kwargs = {'date_start':datetime(2019,5,30), 'date_lim':'20191031'}
		)

	task_e3 = PythonVirtualenvOperator(
		task_id='extract3',
		requirements=REQ,
		system_site_packages=False,
		python_callable=get_parq,
		op_kwargs = {'date_start':datetime(2019,10,31), 'date_lim':'20191231'}
		)

	task_t = PythonVirtualenvOperator(
		task_id='transform',
		requirements=REQ,
		system_site_packages=False,
		python_callable=tra_pvo
        	)
	task_t2 = PythonVirtualenvOperator(
		task_id='month.avg',
		requirements=REQ,
		system_site_packages=False,
		python_callable=avgmonth
		)
	task_l = PythonVirtualenvOperator(
		task_id='load',
		requirements=REQ,
		system_site_packages=False,
		python_callable=ice_cat
		)

#	task_rm_dir = BashOperator(
#		task_id='rm.dir',
#		bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}'
#	)

#	branch_op = BranchPythonOperator(
#		task_id="branch.op",
#		python_callable=branch_func
#	)

	task_start = gen_emp('start')
	task_end = gen_emp('end','all_done')
#Task_extract

#Graph
task_start >> task_e1 
task_e1 >> task_e2
task_e2 >> task_e3 >> task_t >> task_t2 >> task_l >> task_end
