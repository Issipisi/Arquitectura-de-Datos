import psycopg2
import random
# import datetime
from faker import Faker
from faker.providers import address, internet

# Crear instancia de Faker con configuración en español
fake = Faker('es_CL')

# Query para crear la tabla de colaboradores
query1 = """CREATE TABLE IF NOT EXISTS colaboradores (
    id_colaborador SERIAL PRIMARY KEY,
    rut VARCHAR(12) UNIQUE NOT NULL,
    nombre_completo VARCHAR(255) NOT NULL,
    genero CHAR(1) NOT NULL,
    edad INT CHECK (edad >= 18),
    correo_electronico VARCHAR(255) UNIQUE NOT NULL,
    telefono VARCHAR(50),
    direccion_postal VARCHAR(255),
    codigo_postal VARCHAR(10),
    ciudad VARCHAR(500),
    area VARCHAR(500),
    cargo VARCHAR(500),
    sueldo INT
)"""

# Conexión a la base de datos en postgres
connection = psycopg2.connect(
    user="taller2",
    password="joaquintorres1@",
    host="taller2clientes.postgres.database.azure.com",
    port="5432",
    database="core-rrhh"
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

# Función para generar ruts
def generar_rut():
    # Generar un número aleatorio de 8 dígitos
    rut = random.randint(1000000, 39999999)
    
    # Calcular el dígito verificador
    suma = 0
    multiplicador = 2
    for digito in str(rut)[::-1]:
        suma += int(digito) * multiplicador
        multiplicador = multiplicador + 1 if multiplicador < 7 else 2
    dv = 11 - (suma % 11)
    dv = 'k' if dv == 10 else '0' if dv == 11 else str(dv)
    
    return f"{rut}-{dv}"

# Función que genera datos para la tabla de colaboradores
'''
Genera datos ficticios para la tabla 'colaboradores'.
Args:
    cantidad: Número de registros a generar.
Returns:
    None
'''
def generar_datos_colaboradores(cantidad: int):
    cursor = connection.cursor()
    for _ in range(cantidad):
        rut = generar_rut()
        nombre_completo = fake.name()
        genero = fake.random_element(['M', 'F'])
        edad = fake.random_int(18, 90)
        correo_electronico = fake.email()
        telefono = fake.phone_number()
        direccion_postal = fake.street_address()
        codigo_postal = fake.postcode()
        ciudad = fake.city()
        area = fake.job()
        cargo = fake.job()
        sueldo = random.randint(500000, 9999999)
        
        # Crear diccionario de datos
        datos_colaboradores = {
            'rut': rut,
            'nombre_completo': nombre_completo,
            'genero': genero,
            'edad': edad,
            'correo_electronico': correo_electronico,
            'telefono': telefono,
            'direccion_postal': direccion_postal,
            'codigo_postal': codigo_postal,
            'ciudad': ciudad,
            'area': area,
            'cargo': cargo,
            'sueldo': sueldo
        }

        # Insertar datos en la tabla
        query = """INSERT INTO colaboradores (rut, nombre_completo, genero, edad, correo_electronico, telefono, direccion_postal, codigo_postal, ciudad, area, cargo, sueldo)
                    VALUES (%(rut)s, %(nombre_completo)s, %(genero)s, %(edad)s, %(correo_electronico)s, %(telefono)s, %(direccion_postal)s, %(codigo_postal)s, %(ciudad)s, %(area)s, %(cargo)s, %(sueldo)s)"""
        
        try:
            cursor.execute(query, datos_colaboradores)
        except Exception as e:
            print("Error al insertar los datos:", e)

    connection.commit()
    cursor.close()

# Crear la tabla
crear_tabla(query1)

# Generar datos para la tabla de colaboradores
generar_datos_colaboradores(30)

#obtener_datos('colaboradores')

#eliminar_tabla('colaboradores')