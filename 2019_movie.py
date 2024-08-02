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
    '2019 movies',
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
 REQUIREMENTS=["git+https://github.com/5Zigo-Gri-jo/Extract.git@[branch name]"]

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
                 #       op_kwargs=op_kw
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

	task_get_start = gen_emp('get.start')

#Task_extract
	ext_Jan = ext_pvo(
		id = 'ext.jan', func_obj = get_data
	)
        ext_Feb = ext_pvo(
                id = 'ext.feb', func_obj = get_data
        )
        ext_Mar = ext_pvo(
                id = 'ext.mar', func_obj = get_data
        )
        ext_Apr = ext_pvo(
                id = 'ext.apr', func_obj = get_data
        )
        ext_May = ext_pvo(
                id = 'ext.may', func_obj = get_data
        )
        ext_Jun = ext_pvo(
                id = 'ext.jun', func_obj = get_data
        )
        ext_Jul = ext_pvo(
                id = 'ext.jul', func_obj = get_data
        )
        ext_Agu = ext_pvo(
                id = 'ext.agu', func_obj = get_data
        )
        ext_Sep = ext_pvo(
                id = 'ext.sep', func_obj = get_data
        )
        ext_Oct = ext_pvo(
                id = 'ext.oct', func_obj = get_data
        )
        ext_Nov = ext_pvo(
                id = 'ext.nov', func_obj = get_data
        )
        ext_Dec = ext_pvo(
                id = 'ext.dec', func_obj = get_data
        )

#Task
	task_save = PythonVirtualenvOperator(
		task_id='save.data',
		python_callable=save_data,
		system_site_packages=False,	
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

	join = BashOperator(task_id='join',
		bash_command="exit 1",
		trigger_rule='all_done'
	)
	
#Graph

task_start >> branch_op
branch_op >> task_rm_dir >> task_get_start
branch_op >> task_get_start

get_start >> [ext_Jan, ext_Feb, ext_Mar, ext_Apr, ext_May, ext_Jun, ext_Jul, ext_Agu, ext_Sep, ext_Oct, ext_Nov, ext_Dec] 

#ext_Jan >> tra_Jan >> load
#ext_Feb >> tra_Feb >> load
#ext_Mar >> tra_Mar >> load
#ext_Apr >> tra_Apr >> load
#ext_May >> tra_May >> load
#ext_Jun >> tra_Jun >> load
#ext_Jul >> tra_Jul >> load
#ext_Agu >> tra_Agu >> load
#ext_Sep >> tra_Sep >> load
#ext_Oct >> tra_Oct >> load
#ext_Nov >> tra_Nov >> load
#ext_Dec >> tra_Dec >> load

#load >> task_save_data >> task_end
