# weather-forecast-parser
Parser for actual and forecasting weather data from open services

Парсер метеоданных из различных источников (goodmeteo, ru-meteo, rp5, yandex)

- WeatherForecastParser.py - прогнозы погоды
- WeatherParser.py - погода на данный момент

Скрипты ежечасно опрашивают погодные сервисы и сохраняют данные на диск в виде CSV файлов.
Прогнозы хранятся в отдельных директориях согласно названию сервисов (yandex, rp5 и т.п.)
Фактические значения метеоданных хранятся в файле actual_report.csv
Старые варианты парсеров собраны в директории parsers_v1
