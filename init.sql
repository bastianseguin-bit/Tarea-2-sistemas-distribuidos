CREATE DATABASE IF NOT EXISTS mi_base;
USE mi_base;

CREATE TABLE IF NOT EXISTS respuestas (
  id INT AUTO_INCREMENT PRIMARY KEY,
  pregunta TEXT,
  respuesta_dataset TEXT,
  respuesta_llm TEXT,
  score FLOAT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) CHARACTER SET = utf8mb4;