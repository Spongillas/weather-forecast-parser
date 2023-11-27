from bs4 import BeautifulSoup
import requests

import pandas as pd
import re
import os, platform
import warnings
warnings.filterwarnings("ignore")

from datetime import datetime, timedelta
from calendar import monthrange
import time

from abc import ABC, abstractmethod

from typing import Dict, List, Set, Callable, Union


def reconnect(timeout_cnt=0, attempts=5, suspend_time=10) -> Callable:
    print('reconnect')
    
    def _reconnect(func: Callable):
        print('_reconnect')
    # Декоратор для повторения попыток подключения к источнику
    
        def wrapper(*args, **kwargs):
            print('wrapper')
            '''
            timeout_cnt - счетчик неудачных подключений
            attempts - количество попыток подключения
            suspend_time - время ожидания между попытками
            '''

            print('decorator', func)

            try:
                print(func(*args, **kwargs))
                return func(*args, **kwargs)

            except Exception as e:
                print('timeout_cnt ', timeout_cnt)
                timeout_cnt += 1

                if  timeout_cnt >= attempts:
                    print(f"failed to connect, no more trying")
                    return None

                print(f"{func} failed to connect. Attempt: {timeout_cnt}, error: {e}. Waiting {suspend_time}s and repeat...")
                time.sleep(suspend_time)

                wrapper(timeout_cnt=timeout_cnt)

        return wrapper
    
    return _reconnect
    

class Forecast(ABC):
    '''
    Базовый класс для получения прогнозов от разных источников (providers).

    Реализует основные методы:
        get_data - получение обработанного прогноза погоды от источника
        save_data - сохранение обработанного прогноза в виде csv файла
    Дополнительные методы:
        __get_forecast_raw - получение сырых данных от источника + повторные попытки 
    Методы, которые нужно определить в дочерних классах:
        _get_data_from_source - получение сырых данных от источника 
        _extract_data_from_forecast - извлечение из сырых данных информацию о прогнозе        

    '''
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'}       
    
    def __init__(self, provider: str, URL:str = None, **kwargs) -> None:
        """
        Параметры:
            provider        - имя источника
            URL             - URL источника (если нужно получить данные от BeautifulSoup)
            get_and_save    - получение и сохранение данных без вызова дополнительных функций (по умолчанию False)
            data            - обработанный прогноз погоды (pandas dataframe)
            _log_prefix     - префикс для логгирования
            soup            - содержимое ответа на запрос к URL (если указан URL)
        """
        self.URL = URL
        self.provider = provider
        self.data = pd.DataFrame()

        self._log_prefix: str = provider.ljust(10) + '|'
        
        self.soup = self._get_soup()
            
        if kwargs.get('get_and_save', None):
            self.get_data()
            self.save_data()
       
    def get_and_save_data(self) -> None:
        self.get_data()
        self.save_data()

    def get_data(self) -> Union[None, pd.DataFrame]:
        self.soup = self._get_soup()
        forecast_raw = self._get_data_from_source()
        self.data = self._extract_data_from_forecast(forecast_raw) if forecast_raw is not None else None
        return self.data
    
    @reconnect
    def _get_soup(self):
        return BeautifulSoup(requests.get(self.URL, headers=self.headers).content, 'html5lib') if self.URL else None
    
    @abstractmethod
    def _get_data_from_source(self):
        raise NotImplementedError()

    @abstractmethod
    def _extract_data_from_forecast(self):
        raise NotImplementedError()
    
        
    def save_data(self) -> None:
        now = datetime.now()
        
        if platform.system()=='Windows':
            filename = f'{self.provider}\\{now.strftime("%d%m%Y_%H%M")}.csv'
        else:
            filename = f'{self.provider}/{now.strftime("%d%m%Y_%H%M")}.csv'
        
        if not os.path.exists(self.provider):
            os.makedirs(self.provider)

        if self.data is not None:
            self.data.to_csv(filename)
            print(f"{self._log_prefix} {str(now)} Data successfully saved to {filename}")
        else:
            print(f"{self._log_prefix} {str(now)} There are no forecast data to save")
            
class ForecastYandex(Forecast):
    
    def __init__(self, **kwargs) -> None:   
        super().__init__('yandex', URL = "https://yandex.ru/pogoda/?lat=56.813158&lon=60.643738", **kwargs)

    @reconnect    
    def _get_data_from_source(self) -> list:
        forecast_raw = self.soup.find_all('ul', {'class': 'swiper-wrapper'})[0].text
        return re.findall("(\d{1,2}:\d{2})((\+|\-)\d+)([^\,]*)", forecast_raw)
    
            
    def _extract_data_from_forecast(self, forecast_raw: list) -> None:
        forecast_raw = self.__check_time(forecast_raw)

        forecast_list = [{'time': f[0], 
                      'temperature': float(f[1]), 
                      'conditions': re.search('([час]\S*\s)(.*)', f[-1])[2], 
                      'message_debug': f[-1][1:]} for f in forecast_raw]

        data = pd.DataFrame(forecast_list)
        data.index = data.pop('time')

        return data

            
    @staticmethod
    def __trim_hour(forecast_string: str) -> tuple:
        # обрезка часа до одной значащей цифры
        wrong_item = list(forecast_string)
        wrong_item[0] = wrong_item[0][-4:]
        
        return tuple(wrong_item)
    
    
    @staticmethod
    def __is_it_correct_delta(delta: timedelta) -> bool:
        # проверка, что разница во времени корректна
        allowed_values = (timedelta(days=0, hours=1), 
                          timedelta(days=-1, hours=1))
        
        return delta in allowed_values


    def __get_timedelta(self, value2: list, value1: list) -> timedelta:
        # получение разницы во времени (формат '%H:%M') между двумя индексами
        return datetime.strptime(value2[0], '%H:%M') - \
                datetime.strptime(value1[0], '%H:%M')
                
                
    def __check_time(self, forecast_list: list) -> list:
        # Проверка, что все временные метки идут с разницей в 1 час + преобразование в формат datetime
        date = datetime.now().date()
        idxs = [datetime.strptime(forecast_list[0][0], '%H:%M').replace(date.year, date.month, date.day)] # первый индекс
        
        for i in range(len(forecast_list)-1):
            try:
                delta = self.__get_timedelta(forecast_list[i+1], forecast_list[i])

            except: # если неправильно распознались границы времени
                forecast_list[i+1] = self.__trim_hour(forecast_list[i+1])
                delta = self.__get_timedelta(forecast_list[i+1], forecast_list[i])
                
            if not self.__is_it_correct_delta(delta):
                # если неправильно распознались границы времени 
                forecast_list[i+1] = self.__trim_hour(forecast_list[i+1]) 
                delta = self.__get_timedelta(forecast_list[i+1], forecast_list[i])
                
                if not self.__is_it_correct_delta(delta): # если обрезка не помогла
                    raise AttributeError(f"Wrong time sequence: {forecast_list[i][0]}, {forecast_list[i+1][0]}")
                    
            # переход на следующий день
            if delta == timedelta(days=-1, hours=1):
                date += timedelta(days=1)
            
            idxs.append(datetime.strptime(forecast_list[i+1][0], '%H:%M').replace(date.year, date.month, date.day))
                        
        return [(str(dt), *forecast[1:]) for forecast, dt in zip(forecast_list, idxs)]
            
            
class ForecastRumeteo(Forecast):
    
    def __init__(self, **kwargs) -> None:
        super().__init__('rumeteo', URL = "https://ru-meteo.ru/ekaterinburg/hour", **kwargs)

    @reconnect    
    def _get_data_from_source(self) -> pd.DataFrame:
        forecast_table = pd.read_html(self.URL, encoding="UTF-8", header=0)
        forecast = self.__forecast_from_table(forecast_table[0])
            
        for i, table in enumerate(forecast_table[1:]):
            # данные по разным дням находятся в разных таблицах
            next_forecast = self.__forecast_from_table(table, i)
            forecast = pd.concat([forecast, next_forecast])  

        return forecast
        
    @staticmethod
    def _extract_data_from_forecast(forecast_raw: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data['temperature'] = forecast_raw['column0'].apply(lambda x: re.search('(\+|\-)\d+\.?\d?', x)[0])
        data['conditions'] = forecast_raw['column1'].apply(lambda x: x.replace(',', ''))
        data['precipitation'] = forecast_raw['Осадки']
        data['wind_speed'] = forecast_raw['Ветер'].apply(lambda x: int(re.search('\d{1,2}', x)[0]))
        data['wind_direction'] = forecast_raw['Ветер'].apply(lambda x: x.split(', ')[1])
        data['pressure'] = forecast_raw['Давление']
        data['humidity'] = forecast_raw['Влажность'].apply(lambda x: int(re.search('\d{2,3}', x)[0]))  

        return data
          
      
    @staticmethod
    def __forecast_from_table(forecast: pd.DataFrame, add_day: int =-1) -> pd.DataFrame:
        # Получение данных из таблицы за определенный день
        out = forecast.drop(forecast.shape[0]-1).dropna(how='all', axis=1)
        out.index = out[out.columns[0]].apply(lambda x: pd.to_datetime(re.search('\d{2}\:\d{2}', x)[0]) + pd.to_timedelta(f'{add_day+1}D'))
        out.index.name = 'time'
        out.columns = ['column0', 'column1', 'Осадки', 'Ветер', 'Давление', 'Влажность']
    
        return out    
            

class ForecastRp5(Forecast):
    
    def __init__(self, **kwargs) -> None: 
        self._forecast_table = 'forecastTable_1_3'
        super().__init__(
            'rp5', 
            URL = "https://rp5.ru/%D0%9F%D0%BE%D0%B3%D0%BE%D0%B4%D0%B0_%D0%B2_%D0%95%D0%BA%D0%B0%D1%82%D0%B5%D1%80%D0%B8%D0%BD%D0%B1%D1%83%D1%80%D0%B3%D0%B5", 
            **kwargs
            )                
    
    def _extract_data_from_forecast(self, forecast_raw: pd.DataFrame) -> None:
        data = pd.DataFrame()
        data['temperature'] = forecast_raw['Температура, °C']
        data['wind_speed'] = forecast_raw['Ветер: скорость, м/с']
        data['wind_direction'] = forecast_raw['направление']
        data['pressure'] = forecast_raw['Давление, мм рт. ст.']

        data_len = data.shape[0] # Для контроля равенства длин основого и дополнительного прогнозов

        conditions, cloudiness = self.__get_cloudiness()
        data['conditions'] = conditions[-data_len:]
        data['cloudiness'] = cloudiness[-data_len:]

        precipitation, precipitation_info = self.__get_precipitation()
        data['precipitation'] = precipitation[-data_len:]
        data['precipitation_info'] = precipitation_info[-data_len:]
        data['humidity'] = self.__get_humidity()[-data_len:]


        data.index = self.__get_datetime_indexes(forecast_raw.index, forecast_raw['Местное время'])
        data.index.name = 'time'    

        return data
        
    @reconnect    
    def _get_data_from_source(self) -> None:        
        forecast = pd.read_html(self.URL, encoding="UTF-8", header = 0, attrs = {'id': self._forecast_table})[0]
        forecast.index = forecast.iloc[:, 0]
        forecast = forecast.iloc[:, 1:-1].T 
            
        return forecast    
      
        
    def __get_datetime_indexes(self, day_index: list, hour: list) -> list:
        # преобразование индексов у прогноза в формат datetime
        days = [re.search('\d{1,2}\s\w+', x)[0] for x in day_index]
        now = datetime.now()

        days_before_end = monthrange(now.year, now.month)[1] - now.day

        days_diff = [int(day.split()[0]) - now.day if int(day.split()[0]) >= now.day else \
                     int(day.split()[0]) + days_before_end for day in days]

        index = [datetime.strptime(f"{now.year}-{now.month}-{now.day} {h}:00", '%Y-%m-%d %H:%M') +\
                 timedelta(days=d) for d, h in zip(days_diff, hour)]

        return index
    
    def __get_hours(self, raw_number: int = 1) -> tuple:
        # извлечение часов в сутках (для отладки)
        table = self.soup.find('table', {'id': self._forecast_table}).find_all('tr')[raw_number].find_all('td')[1:-1]
        result = []
        
        for i in range(len(table)):
            try:
                result.append(table[i].text)       

            except Exception as e:
                print(f'{self._log_prefix} Error while parsing hours in table column {i}:', e)
                result.append(None)
                continue
                
        return result
    
    def __get_cloudiness(self, raw_number: int = 2) -> tuple:
        '''
        Извлечение облачности. 
        raw_number - номер строки с данными в таблице прогноза
        '''
        table = self.soup.find('table', {'id': self._forecast_table}).find_all('tr')[raw_number].find_all('td')[1:-1]
        result = {'brief': [], 'detailed': []}
        
        for i, element in enumerate(table):
            try:
                result_raw = element.find('div', {'class': 'cc_0'}).find_all('div')[0]['onmouseover']  
                result_info = re.search("<b>(.+)</b><br/>\(([^')]+)", result_raw)
                result['brief'].append(result_info[1] if result_raw else None)
                result['detailed'].append(result_info[2] if result_raw else None)         

            except Exception as e:
                print(f'{self._log_prefix} Error while parsing cloudiness in table column {i}:', e)
                result['brief'].append(None)
                result['detailed'].append(None)
                continue
                
        return result['brief'], result['detailed']
    
    def __get_precipitation(self, raw_number: int = 3) -> tuple:
        # извлечение осадков. raw_number - номер строки с данными в таблице прогноза
        table = self.soup.find('table', {'id': self._forecast_table}).find_all('tr')[raw_number].find_all('td')[1:-1] # Осадки
        result = {'brief': [], 'detailed': []}
        
        for i in range(len(table)):
            try:
                result_raw = table[i].find('div', {'class': 'pr_0'})['onmouseover']
                result_info = re.search("((?:\w+\s?)+)(\(.+\))?", result_raw.split(", '")[1])
                result['brief'].append(result_info[1] if result_raw else None)
                result['detailed'].append(result_info[2] if result_info[2] else None)       

            except Exception as e:
                print(f'{self._log_prefix} Error while parsing precipitation in table column {i}:', e)
                result['brief'].append(None)
                result['detailed'].append(None)
                continue
                
        return result['brief'], result['detailed']
    
    def __find_forecast_row_by_name(self, name: str) -> int: 
        # поиск в таблице прогноза индекса строки с заданным параметром
        for row in self.soup.find('table', {'id': self._forecast_table}).find_all('tr'):
            if row.find_all('td')[0].text.lower().find(name) != -1:
                return row
        
    def __get_humidity(self) -> list:
        # извлечение влажности

        table = self.__find_forecast_row_by_name('влажность').find_all('td')[1:-1]
        result = []
        
        for i in range(len(table)):
            try:
                result.append(table[i].text)       

            except Exception as e:
                print(f'{self._log_prefix} Error while parsing humidity in table column {i}:', e)
                result.append(None)
                continue
                
        return result
        
        
class ForecastGoodmeteo(Forecast):    
    
    def __init__(self, **kwargs) -> None:
        super().__init__('goodmeteo', **kwargs)
        
    @staticmethod
    def _extract_data_from_forecast(forecast_raw: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data['temperature'] = forecast_raw['Температура'].apply(lambda x: float(re.search('\-?\d{1,2}(,|.)\d', x)[0].replace(',','.')))
        data['wind_direction'] = forecast_raw['Ветер'].apply(lambda x: x.split(', ')[1])
        data['wind_speed'] = forecast_raw['Ветер'].apply(lambda x: float(re.search('\d{1,2}(,|.)?\d?', x)[0].replace(',','.')))
        data['humidity'] = forecast_raw['Влажность'].apply(lambda x: int(re.search('\d{1,2}', x)[0]))
        data['pressure'] = forecast_raw['Давление'].apply(lambda x: int(re.search('\d{3}', x)[0]))
        data['cloudiness'] = forecast_raw['Облачность'].apply(lambda x: int(re.search('\d{1,2}', x)[0]))
        data['conditions'] = forecast_raw['Осадки']
        return data

    @reconnect    
    def _get_data_from_source(self) -> pd.DataFrame:
        URL = "https://goodmeteo.ru/pogoda-ekaterinburg/"
        today = pd.read_html(URL, encoding="UTF-8", header=0, index_col=0, parse_dates=True)[0]

        URL = "https://goodmeteo.ru/pogoda-ekaterinburg/zavtra/"
        tomorrow = pd.read_html(URL, encoding="UTF-8", header=0, index_col=0, parse_dates=True)[0]
        tomorrow.index = tomorrow.index + pd.Timedelta('1D') 

        return pd.concat([today, tomorrow])
        

if __name__ == '__main__':  
    rp5 = ForecastRp5()
    yandex = ForecastYandex()
    goodmeteo = ForecastGoodmeteo()
    rumeteo = ForecastRumeteo()  

    while True:
        rp5.get_and_save_data()
        yandex.get_and_save_data()
        goodmeteo.get_and_save_data()
        rumeteo.get_and_save_data()

        time.sleep(3600)