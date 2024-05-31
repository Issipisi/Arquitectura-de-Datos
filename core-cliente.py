import psycopg2
import random
# import datetime
from faker import Faker
from faker.providers import address, internet

# Crear instancia de Faker con configuración en español
fake = Faker('es_ES')

# Queries para crear tablas
query1 = """CREATE TABLE IF NOT EXISTS clientes_demograficos (
    id_cliente SERIAL PRIMARY KEY,
    nombre_completo VARCHAR(255) NOT NULL,
    genero CHAR(1) NOT NULL,
    edad INT CHECK (edad >= 18),
    correo_electronico VARCHAR(255) UNIQUE NOT NULL,
    telefono VARCHAR(20),
    direccion_postal VARCHAR(255),
    codigo_postal VARCHAR(10),
    ciudad VARCHAR(300),
    pais VARCHAR(300),
    ocupacion VARCHAR(300),
    nivel_educativo VARCHAR(200),
    ingresos DECIMAL(10, 2)
)"""

query2 = """CREATE TABLE IF NOT EXISTS clientes_psicograficos (
    id_cliente SERIAL PRIMARY KEY,
    intereses_deportivos TEXT,
    nivel_actividad_fisica VARCHAR(300),
    estilo_vida VARCHAR(300),
    valores_creencias TEXT,
    personalidad VARCHAR(300),
    FOREIGN KEY (id_cliente) REFERENCES clientes_demograficos(id_cliente)
)"""

query3 = """CREATE TABLE IF NOT EXISTS clientes_adicionales (
    id_cliente SERIAL PRIMARY KEY,
    numero_identificacion_personal VARCHAR(20),
    fecha_registro_cliente DATE NOT NULL,
    fecha_ultima_actualizacion DATE,
    consentimiento_datos_personales BOOLEAN NOT NULL DEFAULT false,
    FOREIGN KEY (id_cliente) REFERENCES clientes_demograficos(id_cliente)
)"""

# Conexión a la base de datos en postgres
connection = psycopg2.connect(
    user="taller2",
    password="joaquintorres1@",
    host="taller2clientes.postgres.database.azure.com",
    port="5432",
    database="core-clientes"
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

def generar_datos_demograficos(num_registros):
    """
    Genera datos demográficos ficticios para la tabla 'clientes_demograficos'.
    Args:
        num_registros: Número de registros a generar.
    Returns:
        Lista de diccionarios con datos demográficos.
    """
    cursor = connection.cursor()
    
    for _ in range(num_registros):
        # Generar datos demográficos
        nombre_completo = fake.name()
        genero = fake.random_element(['M', 'F'])
        # fecha_nacimiento = fake.date_between(start_date='-44y', end_date='-19y')  # Años entre 1980 y 2005
        # edad = datetime.date.today().year - fecha_nacimiento.year
        edad = fake.random_int(min=18, max=90)
        correo_electronico = fake.email()
        telefono = fake.phone_number()
        direccion_postal = fake.address()
        codigo_postal = fake.postcode()
        ciudad = fake.city()
        pais = fake.country()
        ocupacion = fake.job()
        nivel_educativo = fake.random_element(['Básica completa', 'Media completa', 'Educación superior', 'Estudiante'])
        ingresos = round(random.uniform(1500, 5000), 2)

        # Crear diccionario con datos
        dato_demografico = {
            'nombre_completo': nombre_completo,
            'genero': genero,
            # 'fecha_nacimiento': fecha_nacimiento,
            'edad': edad,
            'correo_electronico': correo_electronico,
            'telefono': telefono,
            'direccion_postal': direccion_postal,
            'codigo_postal': codigo_postal,
            'ciudad': ciudad,
            'pais': pais,
            'ocupacion': ocupacion,
            'nivel_educativo': nivel_educativo,
            'ingresos': ingresos
        }

        # Insertar datos en la base de datos
        query = """INSERT INTO clientes_demograficos 
                   (nombre_completo, genero, edad, correo_electronico, telefono, direccion_postal, 
                    codigo_postal, ciudad, pais, ocupacion, nivel_educativo, ingresos) 
                   VALUES (%(nombre_completo)s, %(genero)s, %(edad)s, %(correo_electronico)s, %(telefono)s, %(direccion_postal)s,
                           %(codigo_postal)s, %(ciudad)s, %(pais)s, %(ocupacion)s, %(nivel_educativo)s, %(ingresos)s)"""
        
        try:
            cursor.execute(query, dato_demografico)
        except Exception as e:
            print("Error al insertar datos:", e)

    # Guardar los cambios y cerrar el cursor
    connection.commit()
    cursor.close()

def generar_datos_clientes_psicograficos(num_registros):
    """
    Genera datos psicográficos ficticios para la tabla 'clientes_psicograficos'.
    Args:
        num_registros: Número de registros a generar.
    Returns:
        Lista de diccionarios con datos psicográficos.
    """
    cursor = connection.cursor()
    
    for _ in range(num_registros):
        # Generar datos psicográficos
        # id_cliente = fake.random_int(min=1, max=100)
        intereses_deportivos = fake.random_element(['Fútbol', 'Baloncesto', 'Natación', 'Ciclismo', 'Running'])
        nivel_actividad_fisica = fake.random_element(['Bajo', 'Moderado', 'Alto'])
        estilo_vida = fake.random_element(['Sedentario', 'Activo', 'Muy activo'])
        valores_creencias = fake.random_element(['Familia', 'Amistad', 'Trabajo', 'Salud'])
        personalidad = fake.random_element(['Introvertido', 'Extrovertido', 'Analítico', 'Creativo'])

        # Crear diccionario con datos
        dato_psicografico = {
            # 'id_cliente': id_cliente,
            'intereses_deportivos': intereses_deportivos,
            'nivel_actividad_fisica': nivel_actividad_fisica,
            'estilo_vida': estilo_vida,
            'valores_creencias': valores_creencias,
            'personalidad': personalidad
        }

        # Insertar datos en la base de datos
        query = """INSERT INTO clientes_psicograficos 
                   (intereses_deportivos, nivel_actividad_fisica, estilo_vida, valores_creencias, personalidad) 
                   VALUES (%(intereses_deportivos)s, %(nivel_actividad_fisica)s, %(estilo_vida)s, %(valores_creencias)s, %(personalidad)s)"""
        
        try:
            cursor.execute(query, dato_psicografico)
        except Exception as e:
            print("Error al insertar datos:", e)

    # Guardar los cambios y cerrar el cursor
    connection.commit()
    cursor.close()

def generar_datos_adicionales(num_registros):
    """
    Genera datos adicionales ficticios para la tabla 'clientes_adicionales'.
    Args:
        num_registros: Número de registros a generar.
    Returns:
        Lista de diccionarios con datos adicionales.
    """
    cursor = connection.cursor()
    
    for _ in range(num_registros):
        # Generar datos adicionales
        # id_cliente = fake.random_int(min=1, max=1000)
        numero_identificacion_personal = fake.random_int(min=1000000000, max=9999999999)
        # foto_perfil = fake.binary(length=1024)
        fecha_registro_cliente = fake.date_between(start_date='-1y', end_date='today')
        fecha_ultima_actualizacion = fake.date_between(start_date='-1y', end_date='today')
        consentimiento_datos_personales = fake.boolean(chance_of_getting_true=80)

        # Crear diccionario con datos
        dato_adicional = {
            # 'id_cliente': id_cliente,
            'numero_identificacion_personal': numero_identificacion_personal,
            # 'foto_perfil': foto_perfil,
            'fecha_registro_cliente': fecha_registro_cliente,
            'fecha_ultima_actualizacion': fecha_ultima_actualizacion,
            'consentimiento_datos_personales': consentimiento_datos_personales
        }

        # Insertar datos en la base de datos
        query = """INSERT INTO clientes_adicionales 
                   (numero_identificacion_personal, fecha_registro_cliente, fecha_ultima_actualizacion, consentimiento_datos_personales) 
                   VALUES (%(numero_identificacion_personal)s, %(fecha_registro_cliente)s, %(fecha_ultima_actualizacion)s, %(consentimiento_datos_personales)s)"""
        
        try:
            cursor.execute(query, dato_adicional)
        except Exception as e:
            print("Error al insertar datos:", e)

    # Guardar los cambios y cerrar el cursor
    connection.commit()
    cursor.close()
'''
# Crear las tablas
crear_tabla(query1)
crear_tabla(query2)
crear_tabla(query3)

# Generar datos demográficos
generar_datos_demograficos(500)
generar_datos_clientes_psicograficos(100)
generar_datos_adicionales(100)
'''
# Obtener datos de las tablas
obtener_datos('clientes_demograficos')
obtener_datos('clientes_psicograficos')
obtener_datos('clientes_adicionales')
'''
eliminar_tabla('clientes_demograficos')
eliminar_tabla('clientes_psicograficos')
eliminar_tabla('clientes_adicionales')
'''