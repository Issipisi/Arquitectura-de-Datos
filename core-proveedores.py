import psycopg2
import random
# import datetime
from faker import Faker
from faker.providers import address, internet

# Crear instancia de Faker con configuración en español
fake = Faker('es_CL')

# Query para crear la tabla de proveedores con los campos id_proveedor, nombre, nombre contacto, correo contacto, teléfono contacto, dirección tipo de producto
query1 = """CREATE TABLE IF NOT EXISTS proveedores (
    id_proveedor SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    nombre_contacto VARCHAR(255) NOT NULL,
    correo_contacto VARCHAR(255) UNIQUE NOT NULL,
    telefono_contacto VARCHAR(50),
    direccion VARCHAR(255),
    tipo_producto VARCHAR(500)
)"""

# Conexión a la base de datos en postgres
connection = psycopg2.connect(
    user="taller2",
    password="joaquintorres1@",
    host="taller2clientes.postgres.database.azure.com",
    port="5432",
    database="core-proveedores"
    )
    
connection.autocommit = True

# Función para crear tablas
def crear_tabla(query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Tabla creada con éxito")
    except Exception as e:
        print("Error al crear la tabla:", e)
    finally:
        cursor.close()

# Función para obtener datos de una tabla
def obtener_datos(tabla):
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM {tabla}")
        datos = cursor.fetchall()
        for dato in datos:
            print(dato)
    except Exception as e:
        print("Error al obtener los datos:", e)
    finally:
        cursor.close()

# Función para eliminar tablas
def eliminar_tabla(tabla):
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {tabla} CASCADE")
        print("Tabla eliminada con éxito")
    except Exception as e:
        print("Error al eliminar la tabla:", e)
    finally:
        cursor.close()

# Función que genera datos para la tabla de proveedores
#Tipo productos se refiere a los productos que se pueden encontrar en una tienda de implemetos deportivos.
'''
Genera datos ficticios para la tabla 'proveedores'.
Args:
    cantidad: Número de registros a generar.
Returns:
    None
'''
def generar_datos_proveedores(cantidad: int):
    cursor = connection.cursor()
    for _ in range(cantidad):
        nombre = fake.company()
        nombre_contacto = fake.name()
        correo_contacto = fake.email()
        telefono_contacto= fake.phone_number()
        direccion = fake.street_address()
        tipo_producto = fake.random_element(['Fitness', 'Montana', 'Ciclismo', 'Colectivos', 'Nieve', 'Patinaje', 'Combate', 'Precision', 'Acuatico'])
        
        # Crear diccionario de datos
        datos_proveedores = {
            'nombre': nombre,
            'nombre_contacto': nombre_contacto,
            'correo_contacto': correo_contacto,
            'telefono_contacto': telefono_contacto,
            'direccion': direccion,
            'tipo_producto': tipo_producto
        }

        # Insertar datos en la tabla
        query = """INSERT INTO proveedores (nombre, nombre_contacto, correo_contacto, telefono_contacto, direccion, tipo_producto)
                    VALUES (%(nombre)s, %(nombre_contacto)s, %(correo_contacto)s, %(telefono_contacto)s, %(direccion)s, %(tipo_producto)s)"""
        
        try:
            cursor.execute(query, datos_proveedores)
        except Exception as e:
            print("Error al insertar los datos:", e)

    connection.commit()
    cursor.close()


# Crear la tabla
# crear_tabla(query1)

# Generar datos para la tabla de proveedores
# generar_datos_proveedores(30)

# Obtener datos de la tabla de proveedores
# obtener_datos('proveedores')

# Eliminar la tabla de proveedores
# eliminar_tabla('proveedores')
