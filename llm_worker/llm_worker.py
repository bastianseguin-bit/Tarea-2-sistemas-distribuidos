import os
import json
import requests
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPIC_PREGUNTAS = "topic_preguntas_nuevas"
TOPIC_REINTENTOS = "topic_preguntas_reintento"
TOPIC_RESPUESTAS = "topic_respuestas_llm_exitosas"

def main():
    kafka_host = os.getenv("KAFKA_HOST", "kafka:9092")
    ollama_host = os.getenv('OLLAMA_HOST')
    if not ollama_host:
        print("ERROR: Variable OLLAMA_HOST no definida. Saliendo.")
        return
    ollama_url = f"{ollama_host}/api/generate"
    print("Iniciando LLM Worker...")

    consumidor = None
    productor = None
    intentos = 0
    max_intentos = 10  
    while intentos < max_intentos and consumidor is None:
        try:
            print(f"Intentando conectar a Kafka... (Intento {intentos + 1}/{max_intentos})")
            consumidor = KafkaConsumer(
                TOPIC_PREGUNTAS,
                TOPIC_REINTENTOS,  
                bootstrap_servers=[kafka_host],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='llm-workers-group',
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            productor = KafkaProducer(
                bootstrap_servers=[kafka_host],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Conectado a Kafka en {kafka_host}.")
        except NoBrokersAvailable:
            intentos += 1
            print("Kafka no está listo. Reintentando en 5 segundos...")
            time.sleep(5)  
    if consumidor is None:
        print(f"ERROR: No se pudo conectar a Kafka después de {max_intentos} intentos. El worker no puede iniciar.")
        return

    print(f"LLM Worker esperando preguntas en {TOPIC_PREGUNTAS} y {TOPIC_REINTENTOS}...")

    for message in consumidor:
        try:
            data = message.value
            pregunta = data['pregunta']
            print(f"\nPregunta recibida (Intento {data.get('intentos', 1)}): '{pregunta[:50]}...'")

            res = requests.post(
                ollama_url,
                json={
                    "model": "tinyllama",
                    "stream": False,
                    "prompt": pregunta,
                    "options": {"num_predict": 100}
                },
                timeout=60
            )
            res_data = res.json()
            respuesta_llm = res_data.get('response')

            if res.status_code == 200 and respuesta_llm:
                print(f"Respuesta de Ollama: {respuesta_llm}") 
                print("Enviando a Flink para scoring...")
                data["respuesta_llm"] = respuesta_llm
                productor.send(TOPIC_RESPUESTAS, value=data)
            else:
                print(f"Error de Ollama (Status: {res.status_code}). Re-encolando...")
                productor.send(TOPIC_REINTENTOS, value=data)
        except requests.exceptions.RequestException as e:
            print(f"Error de conexión con Ollama: {e}. Re-encolando...")
            productor.send(TOPIC_REINTENTOS, value=data)
        except Exception as e:
            print(f"Error inesperado procesando mensaje: {e}")
    productor.flush() 

if __name__ == "__main__":
    main()
