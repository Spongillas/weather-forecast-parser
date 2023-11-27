from bs4 import BeautifulSoup
import requests

import pandas as pd
import re
import os

from datetime import datetime
import time   

from typing import Dict, List, Set, Callable
        
def get_data(func: Callable) -> Callable:
    # Декоратор для повторения попыток подключения к источнику
    def wrapper(timeout_cnt=0, attempts=5, suspend_time=10):
        '''
        timeout_cnt - счетчик неудачных подключений
        attempts - количество попыток подключения
        suspend_time - время ожидания между попытками
        '''
        
        try:
            return func()
            
        except Exception as e:
            timeout_cnt += 1

            if  timeout_cnt >= attempts:
                print(f"failed to connect, no more trying")
                return None

            print(f"{func} failed to connect. Attempt: {timeout_cnt}, error: {e}. Waiting {suspend_time}s and repeat...")
            time.sleep(suspend_time)
                
            wrapper(timeout_cnt=timeout_cnt)
        
    return wrapper
        
        
@get_data        
def get_fact_weather_goodmeteo() -> dict:
    result = {}
    log_prefix = 'goodmeteo'
    URL = "https://goodmeteo.ru/pogoda-ekaterinburg/"
    soup = BeautifulSoup(requests.get(URL).content, 'html5lib')
    data = soup.find_all('div', {'class': 'b_pogoda'})
    
    
    if len(data)==0:
        raise Exception('There are no data received from URL')

    for element in data:
        result['time'] = str(datetime.now())
        result['provider'] = 'goodmeteo'
        
        det_pog_b1 = element.find('div', {'class': 'det_pog_b1'})
        
        if det_pog_b1:
            det_pog_temp = det_pog_b1.find('div', {'class': 'det_pog_temp'})
            if det_pog_temp:
                result['temperature'] = re.search('(\+|\-)?\d{1,2}', det_pog_temp.text)[0]
                
            det_pog_desc = det_pog_b1.find('div', {'class': 'det_pog_desc'})
            if det_pog_desc:
                result['conditions'] = re.search('([а-яА-Я ])+', det_pog_desc.text)[0].strip()
                
        det_pog_b2 = element.find('div', {'class': 'det_pog_b2'})
        
        if det_pog_b2:
            for element in det_pog_b2.find_all('div'):
                if element.span.text.lower().find('ветер') != -1:
                    try:
                        text = element.b.text.split(', ')
                        result['wind_speed'] = re.search('\d{1,2}(\,|\.)?\d?', text[0])[0]
                        result['wind_direction'] = text[1] if len(text) > 1 else None
                        
                    except Exception as e:
                        print(f'{log_prefix} Error parsing wind, string: {element.b.text}')
                        print(f'{log_prefix} {e}')
                        result['wind_speed'] = None
                        result['wind_direction'] = None
                        
                    continue

                elif element.span.text.lower().find('влажность') != -1:
                    try:
                        result['humidity'] = re.search('\d{2}', element.b.text)[0]
                        
                    except Exception as e:
                        print(f'{log_prefix} Error parsing humidity, string: {element.b.text}')
                        print(f'{log_prefix} {e}')
                        result['humidity'] = None
                    
                    continue

                elif element.span.text.lower().find('давление') != -1:
                    try:
                        result['pressure'] = re.search('\d{3}', element.b.text)[0]
                    except Exception as e:
                        print(f'{log_prefix} Error parsing pressure, string: {element.b.text}')
                        print(f'{log_prefix} {e}')
                        result['pressure'] = None
                        
                    continue 

    return result

@get_data
def get_fact_weather_rumeteo() -> dict:
    result = {}
    log_prefix = 'rumeteo'
    URL = "https://ru-meteo.ru/ekaterinburg/hour"
    soup = BeautifulSoup(requests.get(URL).content, 'html5lib')
    data = soup.find_all('div', {'class': 'content'})
    
    if len(data)==0:
        raise Exception(f'{log_prefix} There are no data received from URL')

    for element in data:
        result['time'] = str(datetime.now())
        result['provider'] = 'rumeteo'
        
        last_report = element.find('div', {'class': 'wrap_content'}).find('div', {'class': 'last-report'})
        
        if last_report:
            temp = last_report.find('div', {'class': 'current-temp'})      
            result['temperature'] = re.search('(\+|\-)?\d{1,2}', temp.text)[0] if temp else None
                
        conditions = element.find('div', {'class': 'wrap_content'}).find('div', {'class': 'conditions'})
        
        if conditions:
            result['conditions'] = conditions.find('li', {'class': 'condition-descr'}).text

            wind = conditions.find('li', {'title': re.compile('Ветер.*')}).text
            
            if re.search('\d+', wind) == None:
                result['wind_speed'] = 0
                result['wind_direction'] = wind
            
            else:
                wind = re.search('(\d+)\s\м\/\с\,\s([- а-я,]*)', wind.lower())

            try:
                result['wind_speed'] = wind[1]
                result['wind_direction'] = wind[2]
                
            except Exception as e:
                print(f'{log_prefix} Error parsing wind, string: {conditions.text}')
                print(log_prefix, e)
                result['wind_speed'] = None
                result['wind_direction'] = None
                
            try:
                result['pressure'] = re.search('давление:\s(\d{3})', conditions.text.lower())[1]
                
            except Exception as e:
                print(f'{log_prefix} Error parsing pressure, string: {conditions.text}')
                print(log_prefix, e)
                result['pressure'] = None
                
            try:
                result['humidity'] = re.search('влажность\s[а-я]+\:\s*(\d{2,3})', conditions.text.lower())[1]
                
            except Exception as e:
                print(f'{log_prefix }Error parsing humidity, string: {conditions.text}')
                print(log_prefix, e)
                result['humidity'] = None
                            
        
        ext = element.find('div', {'class': 'wrap_content'}).find('div', {'class': 'ext'})
        if ext:
            try:
                for s in ext.find_all('li'):
                    if s.text.lower().find('видимость'):
                        result['visibility'] = s.span.text
                        break

            except Exception as e:
                print(f'{log_prefix} Error parsing visibility, string: {ext.text}')
                print(log_prefix, e)
                result['visibility'] = None

    return result

@get_data
def get_fact_weather_yandex() -> dict:
    result = {}
    log_prefix = 'yandex'
    URL = "https://yandex.ru/pogoda/?lat=56.813158&lon=60.643738"
    soup = BeautifulSoup(requests.get(URL).content, 'html5lib')
    data = soup.find_all('div', {'class': 'card_size_big'})
    
    if len(data)==0:
        raise Exception(f'{log_prefix} There are no data received from URL')

    for element in data:
        result['time'] = str(datetime.now())
        result['provider'] = 'yandex'
        
        fact_temp = element.find('div', {'class': 'fact__temp-wrap'})

        if fact_temp:
            x = fact_temp.find('a')['aria-label'].split(',')
            result['temperature'] = re.search('(\+|\-)?\d+', x[0])[0]
            result['conditions'] = x[2][:-1].lower()
        else:
            result['temperature'] = None
            result['conditions'] = None

        fact_wind = element.find('div', {'class': 'fact__wind-speed'})
        result['wind_speed'] = fact_wind.find('span', {'class': 'wind-speed'}).text.replace(',', '.') if fact_wind else None
        result['wind_direction'] = fact_wind.find('abbr').text if fact_wind else None

        fact_humidity = element.find('div', {'class': 'fact__humidity'})
        result['humidity'] = re.search('\d{2,3}',fact_humidity.text)[0] if fact_humidity else None

        fact_pressure = element.find('div', {'class': 'fact__pressure'})
        result['pressure'] = re.search('\d{3}',fact_pressure.text)[0] if fact_pressure else None


    return result




if __name__ == '__main__':    
    filename = 'actual_report.csv'
    
    while True:
        reports = [get_fact_weather_goodmeteo(), 
                   get_fact_weather_rumeteo(), 
                   get_fact_weather_yandex()]

        data = pd.DataFrame([x for x in reports if x is not None])
        try:
            data.time = data.time.apply(pd.to_datetime)
            data.index = data.pop('time')


            if not os.path.exists(filename):
                data.to_csv(filename)
            else:
                data.to_csv(filename, mode='a', header=False)
        except:
            print(f'There are no data to save, empty Dataframe, shape {data.shape}')

        time.sleep(3600)