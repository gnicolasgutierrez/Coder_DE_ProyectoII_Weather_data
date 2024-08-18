import os
import logging
import requests
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, Column, Float, Integer, String, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Cargar variables de entorno
load_dotenv()

API_KEY = os.getenv('API_KEY')
CITIES = ['Tunuyán', 'Mendoza', 'Buenos Aires', 'Córdoba', 'Rosario', 'La Plata', 'Mar del Plata', 'San Miguel de Tucumán', 'Salta', 'Santa Fe']
API_URL_TEMPLATE = 'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'

# Configuración de SQLAlchemy
DATABASE_URL = f"redshift+psycopg2://{os.getenv('USER')}:{os.getenv('PASSWORD')}@{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DBNAME')}"

# Constantes para validación de datos
MIN_TEMPERATURE = -50
MAX_TEMPERATURE = 60

# Usar el nuevo método recomendado para crear la base
Base = declarative_base()

# Configuración del motor
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

class WeatherData(Base):
    __tablename__ = 'weather_data'
    city = Column(String(50), primary_key=True)
    timestamp = Column(TIMESTAMP, primary_key=True)
    temperature = Column(Float)
    humidity = Column(Integer)
    pressure = Column(Integer)
    weather = Column(String(100))

def create_table():
    try:
        Base.metadata.create_all(engine)
        print("Tabla creada o verificada exitosamente.")
    except Exception as e:
        print(f"Error al crear la tabla: {e}")

def fetch_weather_data():
    weather_data_list = []
    for city in CITIES:
        api_url = API_URL_TEMPLATE.format(city=city, api_key=API_KEY)
        try:
            response = requests.get(api_url)
            response.raise_for_status()  # Lanzar excepción si la respuesta no es exitosa
            weather_data = response.json()

            if 'main' in weather_data:
                # Convertir de Kelvin a Celsius
                temperature = weather_data['main'].get('temp')
                if temperature is not None:
                    temperature -= 273.15
                    # Validar temperatura
                    if MIN_TEMPERATURE <= temperature <= MAX_TEMPERATURE:
                        humidity = weather_data['main'].get('humidity')
                        pressure = weather_data['main'].get('pressure')
                        weather_description = weather_data['weather'][0].get('description', 'Unknown') if 'weather' in weather_data else 'Unknown'

                        # Validar otros campos
                        if humidity is not None and pressure is not None:
                            data = {
                                'city': city,
                                'temperature': temperature,
                                'humidity': humidity,
                                'pressure': pressure,
                                'weather': weather_description,
                                'timestamp': datetime.now()
                            }
                            weather_data_list.append(data)
                        else:
                            print(f"Datos incompletos para {city}: Humidity o Pressure es None")
                    else:
                        print(f"Temperatura fuera del rango para {city}: {temperature}°C")
                else:
                    print(f"Temperatura no encontrada para {city}")
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener datos para {city}: {e}")
    
    # Convertir la lista de diccionarios a un DataFrame
    df = pd.DataFrame(weather_data_list)
    return df

def insert_weather_data(df):
    session = Session()
    try:
        for _, row in df.iterrows():
            weather_entry = WeatherData(
                city=row['city'],
                timestamp=row['timestamp'],
                temperature=row['temperature'],
                humidity=row['humidity'],
                pressure=row['pressure'],
                weather=row['weather']
            )
            session.merge(weather_entry)  # Usa `merge` para insertar o actualizar
        session.commit()
        print("Datos insertados exitosamente en Redshift.")
    except Exception as e:
        print(f"Error al insertar datos: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    print("Inicio del script...")
    create_table()
    weather_df = fetch_weather_data()
    insert_weather_data(weather_df)
