import psycopg2
import random
# import datetime
from faker import Faker
from faker.providers import address, internet

# Crear instancia de Faker con configuración en español
fake = Faker('es_ES')

# Query para crear la tabla de colaboradores
query = """CREATE TABLE IF NOT EXISTS colaboradores (
    id_colaborador SERIAL PRIMARY KEY,
    nombre_completo VARCHAR(255) NOT NULL,
    genero CHAR(1) NOT NULL,
    edad INT CHECK (edad >= 18),
    correo_electronico VARCHAR(255) UNIQUE NOT NULL,
    telefono VARCHAR(20),
    direccion_postal VARCHAR(255),
    codigo_postal VARCHAR(10),
    ciudad VARCHAR(50),
    area VARCHAR(50),
    cargo VARCHAR(50),
    sueldo INT(10)
)"""

# Conexión a la base de datos en postgres
connection = psycopg2.connect(
    user="postgres", 
    password="1234", 
    host="localhost", 
    port="5432", 
    database="core-rrhh"
    )
    