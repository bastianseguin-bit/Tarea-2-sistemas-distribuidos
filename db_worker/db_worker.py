import os
import json
import time
import logging
import mysql.connector
from mysql.connector import Error
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

TOPIC_VALIDADOS = "topic_resultados_validados"

def get_kafka_consumer():
    """Crea un consumidor Kafka con reintentos."""
    kafka_host = os.getenv("KAFKA_HOST", "localhost:9092")
    print(f"Conectando Consumidor a Kafka en {kafka_host}...")
    for _ in range(5): 
        try:
            consumer = KafkaConsumer(
                TOPIC_VALIDADOS,
                bootstrap_servers=[kafka_host],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='db-workers-group',
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            print("Consumidor Kafka conectado.")
            return consumer
        except NoBrokersAvailable:
            print("No se puede conectar a Kafka. Reintentando en 5s...")
            time.sleep(5)
    raise RuntimeError("No se pudo conectar a Kafka.")

def get_db_conn():
    """Intenta conectarse a la base de datos MySQL."""
    print("Conectando con MySQL...")
    try:
        conn = mysql.connector.connect(
            host='mysql_base',
            user='usuario',
            password='pass123',
            database='mi_base',
            charset="utf8mb4"
        )
        if conn.is_connected():
            print("Conectado a MySQL.")
            return conn
    except Error as e:
        print(f"ERROR: No se pudo conectar a MySQL: {e}")
        raise RuntimeError("No se pudo conectar a la base de datos MySQL.")

def main():
    try:
        consumer = get_kafka_consumer()
        conn = get_db_conn()
        cursor = conn.cursor()
    except RuntimeError as e:
        print(f"Error fatal al iniciar: {e}. Abortando.")
        return

    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS respuestas (
          id INT AUTO_INCREMENT PRIMARY KEY,
          pregunta TEXT,
          respuesta_dataset TEXT,
          respuesta_llm TEXT,
          score FLOAT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) CHARACTER SET = utf8mb4;
        """)
        conn.commit()
    except Error as e:
        print(f"Error al crear/verificar la tabla: {e}")
        conn.rollback()

    
    insert_sql = "INSERT INTO respuestas (pregunta, respuesta_dataset, respuesta_llm, score) VALUES (%s, %s, %s, %s)"

    print("DB Worker iniciado. Esperando resultados validados...")
    
    for message in consumer:
        try:
            data = message.value
            print(f"Guardando resultado (Score: {data['score']:.2f}): '{data['pregunta'][:50]}...'")
            
            cursor.execute(insert_sql, (
                data['pregunta'], 
                data['respuesta_dataset'], 
                data['respuesta_llm'], 
                data['score']
            ))
            conn.commit() 
            
        except Error as e:
            print(f"Error al insertar en la BD: {e}. Haciendo rollback.")
            conn.rollback()
        
        except Exception as e:
            print(f"Error inesperado al procesar mensaje: {e}")
            conn.rollback()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()