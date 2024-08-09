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
    codigo_barra VARCHAR(13) UNIQUE NOT NULL,
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
    # port="5432",
    database="core-productos"
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
        codigo_barra = fake.ean13()
        nombre = fake.word()
        descripcion = fake.text()
        marca = fake.company()
        tipo_deporte = fake.random_element(['Fútbol', 'Baloncesto', 'Tenis', 'Béisbol', 'Golf', 'Críquet', 'Hockey',
    'Voleibol', 'Natación', 'Ciclismo', 'Atletismo', 'Rugby', 'Boxeo', 
    'Artes marciales', 'Esgrima', 'Gimnasia', 'Escalada', 'Esquí', 
    'Surf', 'Snowboard', 'Patinaje', 'Bádminton', 'Tenis de mesa', 
    'Bolos', 'Remo', 'Tiro con arco', 'Triatlón', 'Halconería',
    'Pesca', 'Equitación', 'Carreras de autos', 'Motociclismo', 'Parapente'])
        categoria = fake.random_element(['Fitness', 'Montana', 'Ciclismo', 'Colectivos', 'Nieve', 'Patinaje', 'Combate', 'Precision', 'Acuatico'])
        subcategoria = fake.random_element(['Hombre', 'Mujer', 'Nino', 'Nina', 'Unisex'])
        precio = random.randint(10000, 1000000)
        stock = random.randint(0, 100)
        proveedor = fake.company()

        # Crear diccionario de datos
        datos_productos = {
            'codigo_barra': codigo_barra,
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
        query = """INSERT INTO productos (codigo_barra, nombre, descripcion, marca, tipo_deporte, categoria, subcategoria, precio, stock, proveedor)
        VALUES (%(codigo_barra)s, %(nombre)s, %(descripcion)s, %(marca)s, %(tipo_deporte)s, %(categoria)s, %(subcategoria)s, %(precio)s, %(stock)s, %(proveedor)s)"""

            
        try:
            cursor.execute(query, datos_productos)
            print("Datos insertados con éxito")
        except Exception as e:
            print("Error al insertar los datos:", e)

    connection.commit()
    cursor.close()

    # Crear la tabla
crear_tabla(query1)

# Generar datos para la tabla de productos
generar_datos_productos(30)

#obtener_datos('productos')
#eliminar_tabla('productos')