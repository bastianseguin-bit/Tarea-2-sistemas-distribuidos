import os
import json
import logging
from pyflink.common import WatermarkStrategy, SimpleStringSchema, Types, Configuration
from pyflink.datastream import StreamExecutionEnvironment, ProcessFunction, OutputTag
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
from sentence_transformers import SentenceTransformer, util


TOPIC_RESPUESTAS = "topic_respuestas_llm_exitosas"
TOPIC_VALIDADOS = "topic_resultados_validados"
TOPIC_PREGUNTAS = "topic_preguntas_nuevas"  


UMBRAL_SCORE = 0.4 
MAX_INTENTOS = 3

class QualityScorer(ProcessFunction):
    def __init__(self):
    
        self.output_tag_low_score = OutputTag("low-score-output", Types.STRING())
        self.model = None 

    def open(self, runtime_context):
        """ Se llama una vez por worker cuando se inicia. Este es el lugar correcto para cargar modelos pesados. """
        print("Cargando modelo SentenceTransformer DENTRO DEL WORKER...")
        try:
            
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer("all-MiniLM-L6-v2")
            print("Modelo cargado exitosamente en el worker.")
        except Exception as e:
            print(f"ERROR FATAL: No se pudo cargar el modelo en el worker: {e}")
            

    def process_element(self, value, ctx):
        """ Procesa cada mensaje que llega del stream. """
        model = self.model  
        
        if model is None:
            print("ERROR: El modelo no está cargado, saltando elemento.")
            return  

        try:
            data = json.loads(value)

            emb1 = model.encode(data['respuesta_llm'], convert_to_tensor=True)
            emb2 = model.encode(data['respuesta_real'], convert_to_tensor=True)
            score = util.cos_sim(emb1, emb2).item()
            data['score'] = score
            intentos_actuales = data.get('intentos', 1)

            if score >= UMBRAL_SCORE:
                print(f"Score ALTO ({score:.2f}). Enviando a DB.")
                yield json.dumps(data)
            elif intentos_actuales < MAX_INTENTOS:
                print(f"Score BAJO ({score:.2f}). Re-inyectando (Intento {intentos_actuales + 1}).")
                data['intentos'] = intentos_actuales + 1
                retry_data = {
                    "pregunta": data['pregunta'],
                    "respuesta_real": data['respuesta_real'],
                    "intentos": data['intentos']
                }
                try:
                    ctx.output(self.output_tag_low_score, json.dumps(retry_data))
                except Exception as output_e:
                    print(f"ERROR AL ENVIAR SIDE OUTPUT: {output_e}. Contexto PyFlink inestable.")
         
            else:
                print(f"Score BAJO ({score:.2f}). Máx intentos ({MAX_INTENTOS}). Descartando.")
        except Exception as e:
            print(f"ERROR al procesar scoring: {e}. Mensaje: {value}")

def run_flink_job():
    config = Configuration()
    config.set_string("execution.target", "local")
    config.set_string("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    kafka_host = os.getenv("KAFKA_HOST", "kafka:9092")
    print(f"Configurando Job de Flink con Kafka: {kafka_host}")

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_host) \
        .set_topics(TOPIC_RESPUESTAS) \
        .set_group_id("flink-scorers-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_properties({
            "fetch.min.bytes": "1",
            "fetch.max.wait.ms": "500",
            "metadata.max.age.ms": "30000",
            "request.timeout.ms": "30000",
            "session.timeout.ms": "30000",
            "heartbeat.interval.ms": "3000",
            "max.poll.interval.ms": "300000"
        }) \
        .build()

    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source (Respuestas LLM)")

    scorer_process = QualityScorer()
    main_stream = stream.process(scorer_process, output_type=Types.STRING())
    low_score_stream = main_stream.get_side_output(scorer_process.output_tag_low_score)

    kafka_sink_validos = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_host) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_VALIDADOS)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    main_stream.sink_to(kafka_sink_validos).name("Kafka Sink (Válidos -> DB)")

    kafka_sink_reintentos = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_host) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_PREGUNTAS)  
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    low_score_stream.sink_to(kafka_sink_reintentos).name("Kafka Sink (Reintentos -> Preguntas)")

    print("Iniciando Job de Flink...")
    env.execute("Pipeline_Scoring_LLM_Calidad")

if __name__ == "__main__":
    run_flink_job()