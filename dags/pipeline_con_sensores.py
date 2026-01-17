from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

from validador_datos_operator import ValidadorDatosOperator

def procesar_datos():
    print("Procesando datos de ventas...")
    return "Datos procesados"

def generar_reporte():
    print("Generando reporte ejecutivo...")
    return "Reporte generado"

with DAG(
    dag_id='pipeline_con_sensores',
    description='Pipeline que espera archivos antes de procesar',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    esperar_datos = FileSensor(
        task_id='esperar_archivo_datos',
        filepath='/tmp/datos_ventas.csv',
        poke_interval=60,
        timeout=3600,
        mode='poke'
    )

    validar_datos = ValidadorDatosOperator(
        task_id='validar_datos_ventas',
        archivo_entrada='/tmp/datos_ventas.csv',
        umbral_calidad=0.95
    )

    procesar = PythonOperator(
        task_id='procesar_datos_ventas',
        python_callable=procesar_datos
    )

    reporte = PythonOperator(
        task_id='generar_reporte',
        python_callable=generar_reporte
    )

    limpiar = BashOperator(
        task_id='limpiar_archivos',
        bash_command='rm -f /tmp/datos_ventas.csv'
    )

    esperar_datos >> validar_datos >> procesar >> reporte >> limpiar
