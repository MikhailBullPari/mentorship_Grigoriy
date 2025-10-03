from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

'''
На будущее for me
Pushing raw pandas DataFrames into XCom is fragile with the default backend; depending on Airflow version and serialization,
 they may be deserialized as empty or as non-DataFrame objects. 
Best practice is to pass references (paths/URLs) or use a custom XCom backend.
'''
def import_from_csv(ti, filename):
    import os
    import pandas as pd
    csv_df = pd.read_csv(filename, sep= ';')
    key_csv = os.path.basename(filename)
    print(csv_df.head())
    ti.xcom_push(key = key_csv, value = csv_df) # pushes to xcom with the key name, /comments.csv in our case

def import_from_xlsx(ti, filename):
    import pandas as pd
    import os
    xlsx_df = pd.read_excel(filename)
    key_xlsx = os.path.basename(filename)
    print(xlsx_df.head())
    ti.xcom_push(key = key_xlsx, value = xlsx_df) # pushes to xcom with the key name , /users.xlsx in our case
    # added openpyxl in requirements.txt and rerun container and it works

def import_from_api(ti, url):
    import pandas as pd
    df_url = pd.read_json(url)
    print(df_url.head())
    ti.xcom_push(key = 'url', value = df_url) # pushes to xcom with the key name

'''def union_data(ti):
    import pandas as pd
    df1 = ti.xcom_pull(key='comments.csv', task_ids="csv_data_import")
    df2 = ti.xcom_pull(key='users.xlsx', task_ids="xlsx_data_import")
    df3 = ti.xcom_pull(key='url', task_ids="api_data_import")
        # Defensive conversion in case upstream changed to lists/dicts later
    if not isinstance(df1, pd.DataFrame):
        df1 = pd.DataFrame(df1)
    if not isinstance(df2, pd.DataFrame):
        df2 = pd.DataFrame(df2)
    if not isinstance(df3, pd.DataFrame):
        df3 = pd.DataFrame(df3)
    import pandas as pd
    obj1 = ti.xcom_pull(key='comments.csv', task_ids='csv_data_import')
    obj2 = ti.xcom_pull(key='users.xlsx', task_ids='xlsx_data_import')
    obj3 = ti.xcom_pull(key='url', task_ids='api_data_import')

    def to_df(o):
        if isinstance(o, pd.DataFrame):
            return o

        try:
            return pd.DataFrame(o)
        except Exception:
            return pd.DataFrame()

    df1, df2, df3 = map(to_df, [obj1, obj2, obj3])

    if df2.empty or df3.empty:
        raise ValueError("Upstream XCom produced empty frames: users=%s, posts=%s" % (df2.shape, df3.shape))


    print("df2 columns:", list(df2.columns))  
    print("df3 columns:", list(df3.columns))  
    df3 = df3.rename(columns = {'id': 'postId'})
    posts_users = df3.merge(df2, left_on='userId', right_on='id')
    posts_users= posts_users.drop(columns=['id']) 
    posts_users = posts_users.rename(columns = {'title': 'posttitle'})
    df1 = df1.rename(columns = {'id': 'commentId', 'title': 'commenttitle' })
    df1 = df1.drop(columns=['userId'])
    result = posts_users.merge(df1, left_on='postId', right_on='commentId')
    result = result.drop(columns=['commentId'])
    ti.xcom_push(key = 'union_df', value = result)
    '''

with DAG(dag_id='first_task',
         description='Create a new DAG with CSV/XLSX/API imports',
         start_date=datetime(2025,1,1),
         schedule="@daily",
         catchup=False,
         ) as mydag:
    '''task_union_data = PythonOperator (
            task_id = 'task_union_data',
            python_callable=union_data
        )'''
    with TaskGroup(group_id='Extract') as extract_group:
        task_сsv_data = PythonOperator(
            task_id = 'csv_data_import',
            python_callable=import_from_csv,
            op_kwargs= {'filename': '/opt/airflow/dags/comments.csv'} #the path should be from docker (opt)
        )
        task_xlsx_data = PythonOperator(
            task_id='xlsx_data_import',
            python_callable=import_from_xlsx,
            op_kwargs={'filename': '/opt/airflow/dags/users.xlsx'}
        )
        task_api_data = PythonOperator(
            task_id = 'api_data_import',
            python_callable=import_from_api,
            op_kwargs={'url': 'https://jsonplaceholder.typicode.com/posts'}
        )


        task_сsv_data >> task_xlsx_data >> task_api_data

