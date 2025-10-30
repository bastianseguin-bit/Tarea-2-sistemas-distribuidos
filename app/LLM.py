import pandas as pd
import mysql.connector
from mysql.connector import Error
import time
import os
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPIC_PREGUNTAS = "topic_preguntas_nuevas"
TOPIC_REINTENTOS = "topic_preguntas_reintento"
TOPIC_RESPUESTAS = "topic_respuestas_llm_exitosas"
TOPIC_VALIDADOS = "topic_resultados_validados"

def main():
    print("Iniciando Generador de Tráfico...")
    
    kafka_host = os.getenv("KAFKA_HOST", "kafka:9092")
    productor = None
    intentos = 0
    max_intentos = 10

    while intentos < max_intentos and productor is None:
        try:
            print(f"Conectando a Kafka en {kafka_host} (Intento {intentos + 1}/{max_intentos})...")
            productor = KafkaProducer(
                bootstrap_servers=[kafka_host],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Productor Kafka conectado.")
        except NoBrokersAvailable:
            intentos += 1
            print("Kafka no está listo. Reintentando en 5 segundos...")
            time.sleep(5) 
    
    if productor is None:
        print(f"ERROR: No se pudo conectar a Kafka después de {max_intentos} intentos. Abortando.")
        return
    print("Creando tópicos de Kafka (si no existen)...")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=[kafka_host],
            client_id='topic-creator'
        )

        topic_list = [
            NewTopic(name=TOPIC_PREGUNTAS, num_partitions=1, replication_factor=1),
            NewTopic(name=TOPIC_REINTENTOS, num_partitions=1, replication_factor=1),
            NewTopic(name=TOPIC_RESPUESTAS, num_partitions=1, replication_factor=1),
            NewTopic(name=TOPIC_VALIDADOS, num_partitions=1, replication_factor=1)
        ]

        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("Tópicos creados/verificados exitosamente.")
    except TopicAlreadyExistsError:
        print("Los tópicos ya existían, todo bien.")
    except Exception as e:
        print(f"ERROR al crear tópicos: {e}. (Continuando de todos modos...)")
    finally:
        admin_client.close()

    try:
        conn = mysql.connector.connect(
            host='mysql_base',
            user='usuario',
            password='pass123',
            database='mi_base',
            charset="utf8mb4"
        )
        print("Conexión a MySQL exitosa.")
    except Error as e:
        print(f"ERROR: No se pudo conectar a MySQL: {e}. Abortando.")
        productor.close() 
        return

    try:
        df = pd.read_csv("/preguntas/test.csv")
        print(f"Dataset cargado con {len(df)} filas.")
    except FileNotFoundError:
        print("ERROR: No se encontró /preguntas/test.csv. Revisa el volumen.")
        productor.close()
        conn.close()
        return


    try:
        cursor = conn.cursor(buffered=True)
        
        for i in range(10000): 
            print(f"\n--- Procesando Pregunta {i} ---")
            
            random_row = df.sample(1).iloc[0]
            pregunta = str(random_row.iloc[2])
            respuesta_real = str(random_row.iloc[3])

         
            if not pregunta or pregunta.lower() == 'nan':
                print("Pregunta inválida ('nan' o vacía). Saltando fila.")
                continue 
            
            cursor.execute("SELECT respuesta_llm FROM respuestas WHERE pregunta = %s AND score IS NOT NULL", (pregunta,))
            row = cursor.fetchone()
            
            if row:
                print("Pregunta ya existe en la BD (cache hit). Saltando.")
            else:
                print("Pregunta nueva. Enviando a Kafka...")
                mensaje = {
                    "pregunta": pregunta,
                    "respuesta_real": respuesta_real,
                    "intentos": 1 
                }
                
                productor.send(TOPIC_PREGUNTAS, value=mensaje)
                productor.flush() 
            

    except KeyboardInterrupt:
        print("\nProceso interrumpido.")
    except Exception as e:
        print(f"ERROR inesperado en el bucle principal: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexión a MySQL cerrada.")
        if 'productor' in locals():
            productor.close()
            print("Productor Kafka cerrado.")
        print("Generador de tráfico finalizado.")


if __name__ == "__main__":
    main()