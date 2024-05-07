from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from kafka import KafkaProducer
import time
import logging

# from airflow import DAG
# from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Thameur',
    'start_date': datetime(2024,5,3,6,00)
}


def get_data():
    import requests
    response = requests.get("https://randomuser.me/api/")
    response = response.json()['results'][0]
    return response


def format_data(response):
    data = {}
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{response['location']['street']['number']}, " \
                      f"{response['location']['street']['name']}, " \
                      f"{response['location']['city']}, " \
                      f"{response['location']['state']}, " \
                      f"{response['location']['country']}"

    data['postcode'] = response['location']['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data


def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1min
            break
        try:
            response = format_data(get_data())
            print(json.dumps(response, indent=3))
            producer.send('users_created', json.dumps(response).encode('utf-8'))

        except Exception as e:
            logging.error(f'An error occured: {e} ')
            continue


stream_data()
with DAG('thameur_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )