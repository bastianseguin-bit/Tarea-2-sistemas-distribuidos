# Tarea 2 – Sistemas Distribuidos 2025-2
Arquitectura Asíncrona con Kafka y Flink

## Descripción
Este proyecto corresponde al Entregable 2 del curso Sistemas Distribuidos.  
El objetivo fue rediseñar el sistema de la tarea anterior para hacerlo asíncrono y tolerante a fallos, utilizando Apache Kafka y Apache Flink.

El sistema procesa preguntas del dataset de Yahoo! Answers, obtiene respuestas desde un LLM y calcula un score de calidad, reenviando las preguntas de baja calidad para regenerarlas automáticamente.

## Arquitectura
El sistema se compone de los siguientes módulos:

1. Traffic Generator → envía preguntas a Kafka.  
2. LLM Consumer → obtiene las preguntas desde Kafka, llama al modelo y envía la respuesta.  
3. Flink Job → analiza las respuestas, calcula el score y decide si reenviar o almacenar.  
4. Storage Service → guarda las respuestas finales en la base de datos.  
5. Kafka + Zookeeper → manejan la comunicación asíncrona entre servicios.  

Flujo general:
```
Traffic Generator → Kafka → LLM Consumer → Flink → Kafka → Storage
```

## Estrategia de Reintento
- Errores de sobrecarga: reintento con exponential backoff.  
- Errores de cuota: reintento diferido.  
- Se limita el número de reintentos por pregunta para evitar bucles infinitos.

## Ejecución con Docker
### Requisitos
- Docker y Docker Compose instalados.

### Pasos
```bash
git clone https://github.com/bastianseguin-bit/Tarea-2-sistemas-distribuidos.git
cd Tarea-2-sistemas-distribuidos
docker-compose up --build
```

Esto levantará todos los servicios: Kafka, Flink, LLM Consumer, Storage y el generador de tráfico.

## Resultados
- Sistema asíncrono y más resiliente ante fallos del LLM.  
- Reducción de la latencia percibida y aumento del throughput.  
- Mejora del score promedio de respuestas tras el feedback loop de Flink.

## Video de Demostración
[Enlace al video de presentación](#)
