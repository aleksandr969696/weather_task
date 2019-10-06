import pandas as pd
import requests
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery
import datetime
import sys





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
        client.load_table_from_dataframe(data, table_ref)
    except FileNotFoundError:
        print('file '+key+' not found')
    except:
        print('Невозможно найти или создать dataset или table с таким названием.')


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


if __name__=='__main__':
    key_address = 'Weather for room42-a7e9f383b611.json'
    # project_id = 'weather-for-room42'
    dataset = 'weather'
    table = 'weather'
    if len(sys.argv)>=2:
        table = sys.argv[1]
    if len(sys.argv)>=3:
        dataset = sys.argv[2]
    if len(sys.argv)>=4:
        key_address = sys.argv[3]
    main(key_address, dataset, table)
