from datetime import timedelta,datetime
import requests
import psycopg2
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd
import smtplib
from email.mime.text import MIMEText


# argumentos por defecto para el DAG
default_args = {
    'owner': 'MollonRodrigo',
    'depends_on_past': False,
    'start_date': datetime(2023,9,1),
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}

# variables de entorno necesarias
WAREHOUSE_HOST     = os.environ.get('WAREHOUSE_HOST')
WAREHOUSE_DBNAME   = os.environ.get('WAREHOUSE_DBNAME')
WAREHOUSE_USER     = os.environ.get('WAREHOUSE_USER')
WAREHOUSE_PASSWORD = os.environ.get('WAREHOUSE_PASSWORD')
OPENWEATHER_APIKEY = os.environ.get('OPENWEATHER_APIKEY')
EMAIL_FROM = os.environ.get('EMAIL_FROM')
EMAIL_TO = os.environ.get('EMAIL_TO')
GMAIL_KEY = os.environ.get('GMAIL_KEY')

BC_dag = DAG(
    dag_id='ETL_ArgOpenWeatherMap',
    default_args=default_args,
    description='Agrega datos de clima de Argentina por Provincia diariamente',
    schedule_interval="@daily",
    catchup=False,
    tags = ["Mollon", "OpenWeatherMap", "Proyecto Final"]
)

def ejecutaET():

        # Defino la lista de ciudades principales de cada provincia argentina para recorrer con un for
    provincias = ['Buenos Aires',#1
        'Catamarca',#2
        'Chaco',#3
        'Comodoro Rivadavia',#4
        'Córdoba',#5
        'Corrientes',#6
        'Entre Ríos',#7
        'Formosa',#8
        'San Salvador de Jujuy',#9
        'La Pampa',#10
        'La Rioja',#11
        'Mendoza',#12
        'Misiones',#13
        'Neuquén',#14
        'Viedma',#15
        'Salta',#16
        'San Juan',#17
        'San Luis',#18
        'Río Gallegos',#19
        'Santa Fe',#20
        'Santiago del Estero',#21
        'Ushuaia',#22
        'Tucumán'#23
        ]

    data_list = []

    for ciudad in provincias:
        url = f'http://api.openweathermap.org/data/2.5/weather?q={ciudad},AR&appid={OPENWEATHER_APIKEY}&units=metric'
        response = requests.get(url)
        
        if response.status_code == 200: #Codigo 200 indica "respuesta satisfactoria" de la API
            data = response.json()
            
            # Extraigo los valores individuales de cada respuesta de la API
            dt = datetime.fromtimestamp(data["dt"]).strftime('%Y-%m-%d %H:%M:%S')
            date = datetime.fromtimestamp(data["dt"]).strftime('%Y-%m-%d')   # Agrego una columna con la fecha en formato "yyyy-mm-dd" a partir del timestamp de dt
            temp = data["main"]["temp"]
            temp_min = data["main"]["temp_min"]
            temp_max = data["main"]["temp_max"]
            pressure = data["main"]["pressure"]
            humidity = data["main"]["humidity"]
            visibility = data.get("visibility")
            feels_like = data["main"].get("feels_like")
            weather_main = data["weather"][0]["main"]
            weather_description = data["weather"][0]["description"]
            wind_speed = data["wind"]["speed"]
            wind_deg = data["wind"]["deg"]
            clouds_all = data["clouds"]["all"]
            sunrise = datetime.fromtimestamp(data["sys"]["sunrise"]).strftime('%Y-%m-%d %H:%M:%S')
            sunset = datetime.fromtimestamp(data["sys"]["sunset"]).strftime('%Y-%m-%d %H:%M:%S')
            timezone = data["timezone"]
            city_id = data["id"]
            
            # Creo el diccionario con los datos de la respuesta formateados
            data_dict = {
                "Provincia": ciudad,
                "dt": dt,
                "date": date, 
                "temp": temp,
                "temp_min": temp_min,
                "temp_max": temp_max,
                "pressure": pressure,
                "humidity": humidity,
                "visibility": visibility,
                "feels_like": feels_like,
                "weather_main": weather_main,
                "weather_description": weather_description,
                "wind_speed": wind_speed,
                "wind_deg": wind_deg,
                "clouds_all": clouds_all,
                "sunrise": sunrise,
                "sunset": sunset,
                "timezone": timezone,
                "city_id": city_id,
            }
            
            # Agregar el diccionario a la lista
            data_list.append(data_dict)
        else:
            continue

    # Crear el DataFrame de Pandas a partir de la lista de diccionarios
    df = pd.DataFrame(data_list)
    # Eliminar duplicados del DataFrame basados en todas las columnas
    df.drop_duplicates(inplace=True)
    # Guarda el dataframe en un csv, si el archivo existia lo sobreescribe
    nombre_archivo = "APIdata_temp.csv"
    df.to_csv(nombre_archivo, index=False, mode='w')
    return nombre_archivo    

# Creo la conexión a la base de datos de Amazon Redshift y cargo los datos
def cargaDatosEnBD():  
    conn = psycopg2.connect(
        user=WAREHOUSE_USER,
        password=WAREHOUSE_PASSWORD,
        host=WAREHOUSE_HOST,
        port='5439',
        database=WAREHOUSE_DBNAME
    )

    #Traigo el contenido del archivo generado en la tarea de extraccion
    df = pd.read_csv("APIdata_temp.csv")
    df["date"] = pd.to_datetime(df["date"])

    # Creo un cursor para ejecutar consultas
    cursor = conn.cursor()

    # Itero por cada fila del DataFrame y realizo la inserción fila por fila en la tabla
    for _, row in df.iterrows():
        # Convierte las fechas en formato "yyyy-mm-dd" de la columna "dt" y "date" a objetos datetime
        row["dt"] = datetime.strptime(row["dt"], '%Y-%m-%d %H:%M:%S')

        # Inserto cada columna en la tabla utilizando una consulta con parámetros
        query = """
        INSERT INTO {table_name} (provincia, fecha_hora, fecha, temperatura_actual, temperatura_minima, temperatura_maxima, presion, humedad, visibilidad, sensacion_termica,
        clima_main_group, descripcion, viento_velocidad, viento_orientacion, nubosidad, salida_del_sol, puesta_del_sol, zona_horaria, ciudad_id)
        VALUES (%(Provincia)s, %(dt)s, %(date)s, %(temp)s, %(temp_min)s, %(temp_max)s, %(pressure)s, %(humidity)s, %(visibility)s,
        %(feels_like)s, %(weather_main)s, %(weather_description)s, %(wind_speed)s, %(wind_deg)s, %(clouds_all)s, %(sunrise)s,
        %(sunset)s, %(timezone)s, %(city_id)s)
        """.format(table_name='climaprovinciasargentina')

        # Ejecuto la consulta con los valores de la fila actual
        try:
            cursor.execute(query, row)
        except psycopg2.IntegrityError as e:
            # Si se genera una excepción imprimo un mensaje de error
            print(f"Excepción al insertar fila: {e}")
            continue
            
    # Confirmo los cambios y cierra la conexión y el cursor
    conn.commit()
    cursor.close()
    conn.close()

def verificar_temperatura():
    # Tomo el archivo generado en la extracción y me fijo si hay temperaturas mayores a un valor
    df = pd.read_csv("APIdata_temp.csv")
    if (df['temp_max'] > 29).any() or (df['temp_min'] < 5).any():
        df_filtrado = (df['temp_max'] > 29) | (df['temp_min'] < 5)
        filas_seleccionadas = df.loc[df_filtrado]
        mensajes = [(fila['Provincia'], 'La máxima es superior a 29°C' if fila['temp_max'] > 29 else 'La mínima es inferior a 5°C') for _, fila in filas_seleccionadas.iterrows()]
        cuerpo_mensaje = "\n".join([f"{provincia}: {mensaje}" for provincia, mensaje in mensajes])
        subject = "Aviso Automático - Temperaturas Altas/Bajas"
    else:
        cuerpo_mensaje = "No se encontraron temperaturas altas o bajas en los datos."
        subject = "Aviso Automático - Sin Temperaturas Altas/Bajas"
    
    body = cuerpo_mensaje
    sender = EMAIL_FROM
    password = GMAIL_KEY
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_FROM
    
    # Con los parámetros seteados envío el mail
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(sender, password)
        smtp_server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())

# Tareas
##1. Extraccion y transforamdo de los datos
ejecutarET = PythonOperator(
    task_id='ejecutaET',
    python_callable=ejecutaET,
    dag=BC_dag,
)

##2. Carga los datos obtenidos a la base de datos
cargaDatosEnBD = PythonOperator(
    task_id='cargaDatosEnBD',
    python_callable=cargaDatosEnBD,
    dag=BC_dag,
)

##3. Envia alerta por temeperatura
verificar_temperatura = PythonOperator(
    task_id='verificar_temperatura',
    python_callable=verificar_temperatura,
    dag=BC_dag,
)

##4. Mensaje de consola de que el proceso de carga termino
procesoCarga_finalizado = BashOperator(
    task_id='procesoCarga_finalizado',
    bash_command='echo "proceso de carga finalizado"',
    dag=BC_dag
)

##5. Mensaje de consola de que el proceso de verificacion de temperatura termino
procesoVerifTemperatura_finalizado = BashOperator(
    task_id='procesoVerifTemperatura_finalizado',
    bash_command='echo "proceso de verificacion de temperatura finalizado"',
    dag=BC_dag
)

# Definicion orden de tareas
ejecutarET >> cargaDatosEnBD >> procesoCarga_finalizado
ejecutarET >> verificar_temperatura >> procesoVerifTemperatura_finalizado