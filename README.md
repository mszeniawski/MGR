# Determinanty opóźnień w ruchu lotniczym - kody źródłowe

Repozytorium kodów źródłowych wykorzystanych w procesie kolekcjonowania danych o ruchu lotniczym na potrzeby pracy magisterskiej.

Repozytorium zawiera następujące kody:
1. **air_traffic_helper.py** - skrypt pobierający dane o ruchu lotniczym z api lotniczego OpenSkyNetwork,
2. **weather_data_helper.py** - skrypt pobierający dane o warunkach atmosferycznych z api pogodowego OpenWeatherMap,
3. **scrap_data_helper.py** - skrypt scrapujący dane o opóźnieniach i rozkładach lotów,
4. **store_data_helper.py** - skrypt zapisujący zebrane dane do nierelacyjnej bazy danych MongoDB,
5. **data_access_helper.py** - skrypt z funkcjami pomocniczymi do pobierania danych z bazy MongoDB,
6. **store_all_continuous_script.py** - skrypt kolekcjonujący dane

## Przygotowanie środowiska uruchomieniowego

Wymagania niezbędne do uruchomienia programu:
* Maszyna z systemem Windows lub Linux
* Python 3
* Pandas
* Requests
* OpenSkyApi (instalacja zgodnie z instrukcją: https://github.com/openskynetwork/opensky-api)
* BeautifulSoup
* Selenium
* ChromeDriver
* Pytz
* Tzlocal 
* Pymongo

Po zainstalowaniu wyżej wymienionych pakietów i narzędzi, należy założyć bezpłatne konto na stronie https://opensky-network.org/ oraz https://openweathermap.org/ w celu uzyskania kluczy dostępu do api. W przypadku api **OpenSkyNetwork** jest to para **userId** i **userPass**, natomiast w api pogodowym **OpenWeatherMap**, jest to **api_key**.

Klucze dostępu, należy dodać w skryptach **air_traffic_helper.py** i **weather_data_helper.py** wpisując ich wartości w funkcji __init__ obu skryptów:

air_traffic_helper.py:
```
def __init__(self):
  self.__api_base_path = "https://opensky-network.org/api/{}"
  self.__user = "USER_ID"
  self.__pass = "USER_PASS"
```

weather_data_helper.py:
```
def __init__(self):
  self.__api_key = "API_KEY"
  self.__api_url_basic = "http://api.openweathermap.org/data/2.5/{}&APPID=" + self.__api_key
```

Ostatnią operacją jest utworzenie bazy danych MongoDB (lokalnie lub w chmurze) oraz dodanie łańcucha znaków definiującego połączenie do niej. Połączenie należy skonfigurować w pliku **store_data_helper.py** w miejscu:
```
mongo_connection = 'MONGO_CONNECTION_STRING'
```

## Uruchomienie kolekcjonowana danych

Po przejściu wyżej wymienionych kroków można uruchomić skrypt kolekcjonujący dane z linii komend:
```
python store_all_continuous_script.py
```
Skrypt zacznie zbierać dane i zapisywać je do bazy danych MongoDB.
