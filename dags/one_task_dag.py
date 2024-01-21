from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def get_data():
    import requests

    res = requests.get('https://randomuser.me/api/')
    res =res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    name = res['name']
    picture = res['picture']
    location = res['location']
    data['gender'] = res['gender']
    data['fname'] = name['first']
    data['lname'] = name['last']
    data['street name'] = location['street']['name']
    data['street number'] = location['street']['number']
    data['city'] = location['city']
    data['country'] = location['country']
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['picture'] = picture['large']
    return data


    


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('user_created',json.dumps(res).encode('utf-8'))
        except Exception as e :
            logging.erro(f'An error occured: {e}')
            continue


"""default_args = {
    'owner' :'daniel',
    'depend_on_past':False,
    'email_on_failure':False,
    'email_on_entry':False,
    'retries':0,
    'catchup':False,
    'start_date':datetime(2024,1,13)
}


with DAG(
    dag_id='one_task',
    description= 'single task',
    schedule_interval = None,
    default_args = default_args
) as dag:
    
    stream_task = PythonOperator(
        task_id = 'stream_task1',
        python_callable = stream_data
    )
"""

stream_data()