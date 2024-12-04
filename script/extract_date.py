#Este script extrae la informacion de ciudades especificas

from dotenv import load_dotenv
import os
import requests
import datetime
import pyodbc

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Variables de configuración desde el archivo .env
API_KEY = os.getenv("OPENWEATHER_API_KEY")
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# URL base de la API de OpenWeatherMap
API_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# Función para extraer datos de la API
def extract_data_from_api(city):
    url = f"{API_BASE_URL}?q={city}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print(f"Datos extraídos exitosamente desde la API para la ciudad: {city}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error en la extracción de datos: {e}")
        return None

# Función para transformar los datos extraídos en un diccionario
def transform_data(data):
    if not data:
        return None

    try:
        transformed_data = {
            "ciudad": data.get("name"),
            "temperatura": float(data["main"]["temp"]),
            "humedad": int(data["main"]["humidity"]),
            "presion": int(data["main"]["pressure"]),
            "descripcion_clima": data["weather"][0]["description"],
            "velocidad_viento": float(data["wind"]["speed"]),
            "marca_temporal": datetime.datetime.now()
        }
        print(f"Datos transformados correctamente: {transformed_data}")
        return transformed_data
    except (KeyError, ValueError) as e:
        print(f"Error al transformar los datos: {e}")
        return None

# Función para cargar los datos transformados a SQL Server
def load_data_to_sqlserver(data):
    if not data:
        print("No hay datos para cargar en la base de datos.")
        return

    try:
        # Crear la cadena de conexión ODBC
        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={DB_SERVER};'
            f'DATABASE={DB_NAME};'
            f'UID={DB_USER};'
            f'PWD={DB_PASSWORD}'
        )

        # Establecer conexión con la base de datos
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Verificar si la tabla 'WeatherData' existe y crearla si no existe
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Weather_db')
            BEGIN
                CREATE TABLE Weather_db (
                    ciudad VARCHAR(100),
                    temperatura FLOAT,
                    humedad INT,
                    presion INT,
                    descripcion_clima VARCHAR(255),
                    velocidad_viento FLOAT,
                    marca_temporal DATETIME
                )
            END
        """)
        conn.commit()
        print("Tabla 'Weather_db' verificada o creada exitosamente.")

        # Insertar los datos transformados en la tabla 'WeatherData'
        cursor.execute("""
            INSERT INTO Weather_db (ciudad, temperatura, humedad, presion, descripcion_clima, velocidad_viento, marca_temporal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            data["ciudad"],
            data["temperatura"],
            data["humedad"],
            data["presion"],
            data["descripcion_clima"],
            data["velocidad_viento"],
            data["marca_temporal"]
        ))

        # Confirmar la inserción
        conn.commit()
        print("Datos cargados exitosamente a la base de datos.")

        # Cerrar la conexión
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error al cargar los datos a SQL Server: {e}")

# Proceso ETL: extracción, transformación y carga
while True:
    ciudad = input("Ingresa el nombre de la ciudad para obtener la temperatura: ")
    data = extract_data_from_api(ciudad)

    if data and data.get("cod") == 200:  # Verifica que la API devuelva un código 200 (éxito)
        transformed_data = transform_data(data)
        load_data_to_sqlserver(transformed_data)
        break
    else:
        print(f"La ciudad '{ciudad}' no fue encontrada. Por favor, intenta nuevamente.")
