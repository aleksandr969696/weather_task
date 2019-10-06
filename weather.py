import pandas as pd
import requests
from google.cloud import bigquery
import datetime
import sys
import argparse





def get_weather(url, params = None, headers = None):
    try:
        res = requests.get(url, params = params, headers = headers)
        res.raise_for_status()
    except requests.HTTPError as err:
        code = err.response.status_code
        print("ошибка url: {0}, code: {1}".format(url, code))
        return ()
    except requests.RequestException:
        print("ошибка подключения к url: ", url)
        return ()
    else:
        data = res.json()
        temp = data['fact']['temp']
        wind_speed = data['fact']['wind_speed']
        pressure = data['fact']['pressure_mm']
        humidity = data['fact']['humidity']
        return (humidity, temp, wind_speed,  pressure)

def load_to_bigquery(data, dataset_, table_,key ):
    try:
        client = bigquery.Client.from_service_account_json(key)
        dataset_ref = client.dataset(dataset_)
        dataset = bigquery.Dataset(dataset_ref)

        dataset = client.create_dataset(dataset, exists_ok=True)
        table_ref = dataset_ref.table(table_)
        result = client.load_table_from_dataframe(data, table_ref).result()
    except FileNotFoundError:
        print('file '+key+' not found')
    except Exception as ex:
        print('error')
        print(ex)

def main(key, dataset, table):
    url = r'https://api.weather.yandex.ru/v1/forecast?lat=55.75396&lon=37.620393'
    headers = {'X-Yandex-API-Key': '9b555871-1c3e-486c-bef5-85059dae8708'}
    weather_data = get_weather(url=url, headers=headers)
    if len(weather_data)==4:
        data = pd.DataFrame({'date': [datetime.datetime.today().date()],'humidity': [weather_data[0]],
                          'temp': [weather_data[1]], 'wind_speed': [weather_data[2]], 'pressure': [weather_data[3]]})
        load_to_bigquery(data, dataset, table, key)
    else:
        return

def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset', default='weather_dataset')
    parser.add_argument('-t', '--table', default='weather_table')
    parser.add_argument('-k', '--key', default='key.json')

    return parser

if __name__=='__main__':
    parser = createParser()
    namespace = parser.parse_args(sys.argv[1:])
    main(namespace.key, namespace.dataset, namespace.table)