import psycopg2
import random
# import datetime
from faker import Faker
from faker.providers import address, internet

# Crear instancia de Faker con configuración en español
fake = Faker('es_CL')

# Query para crear la tabla de productos
query1 = """CREATE TABLE IF NOT EXISTS productos (
    id_producto SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    descripcion TEXT,
    marca VARCHAR(255),
    tipo_deporte VARCHAR(255),
    categoria VARCHAR(255),
    subcategoria VARCHAR(255),
    precio INT,
    stock INT,
    proveedor VARCHAR(255)
)"""

# Conexión a la base de datos en postgres
connection = psycopg2.connect(
    user="taller2",
    password="joaquintorres1@",
    host="taller2clientes.postgres.database.azure.com",
    port="5432",
    database="core-productos "
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

# Función que genera datos para la tabla de productos
def generar_datos_productos(cantidad):
    cursor = connection.cursor()
    for _ in range(cantidad):
        nombre = fake.word()
        descripcion = fake.text()
        marca = fake.company()
        tipo_deporte = fake.word()
        categoria = fake.random_element(['Fitness', 'Montana', 'Ciclismo', 'Colectivos', 'Nieve', 'Patinaje', 'Combate', 'Precision', 'Acuatico'])
        subcategoria = fake.random_element(['Hombre', 'Mujer', 'Nino', 'Nina', 'Unisex'])
        precio = random.randint(10000, 1000000)
        stock = random.randint(0, 100)
        proveedor = fake.company()

        # Crear diccionario de datos
        datos_productos = {
            'nombre': nombre,
            'descripcion': descripcion,
            'marca': marca,
            'tipo_deporte': tipo_deporte,
            'categoria': categoria,
            'subcategoria': subcategoria,
            'precio': precio,
            'stock': stock,
            'proveedor': proveedor
        }

        # Insertar datos en la tabla
        query = """INSERT INTO productos (nombre, descripcion, marca, tipo_deporte, categoria, subcategoria, precio, stock, proveedor)
        VALUES (%(nombre)s, %(descripcion)s, %(marca)s, %(tipo_deporte)s, %(categoria)s, %(subcategoria)s, %(precio)s, %(stock)s, %(proveedor)s)"""

            
        try:
            cursor.execute(query, datos_productos)
            print("Datos insertados con éxito")
        except Exception as e:
            print("Error al insertar los datos:", e)

    connection.commit()
    cursor.close()