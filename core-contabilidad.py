import datetime
import psycopg2
import random
# import datetime
from faker import Faker

# Crear instancia de Faker con configuración en español
fake = Faker('es_CL')

# Query para crear la tabla de contabilidad

query1 = """CREATE TABLE IF NOT EXISTS contabilidad (
    id_transaccion SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    descripcion VARCHAR(100) NOT NULL,
    monto DECIMAL NOT NULL,
    moneda VARCHAR(5) NOT NULL,
    id_cuenta_origen INT NOT NULL,
    id_cuenta_destino INT NOT NULL,
    tipo_transaccion VARCHAR(20) NOT NULL
)"""

# Conexión a la base de datos en postgres
connection = psycopg2.connect(
    user="taller2",
    password="joaquintorres1@",
    host="taller2clientes.postgres.database.azure.com",
    # port="5432",
    database="core-contabilidad"
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
def generar_datos_contabilidad(cantidad):
    cursor = connection.cursor()
    for _ in range(cantidad):
        fecha = fake.date()
        descripcion = fake.sentence()
        monto = random.randint(1, 1000000)
        moneda = fake.random_element(["CLP","USD","EUR"])
        id_cuenta_origen = random.randint(1, 300)
        id_cuenta_destino = random.randint(1, 300)
        tipo_transaccion = fake.random_element(["ingreso","gasto","transferencia"])

        # crear diccionario de datos
        datos_contabilidad = {
            "fecha": fecha,
            "descripcion": descripcion,
            "monto": monto,
            "moneda": moneda,
            "id_cuenta_origen": id_cuenta_origen,
            "id_cuenta_destino": id_cuenta_destino,
            "tipo_transaccion": tipo_transaccion
        }

        # insertar datos en la tabla
        query = """INSERT INTO contabilidad (fecha, descripcion, monto, moneda, id_cuenta_origen, id_cuenta_destino, tipo_transaccion)
        VALUES (%(fecha)s, %(descripcion)s, %(monto)s, %(moneda)s, %(id_cuenta_origen)s, %(id_cuenta_destino)s, %(tipo_transaccion)s)"""

        try:
            cursor.execute(query, datos_contabilidad)
            print("Datos insertados con éxito")
        except Exception as e:
            print("Error al insertar los datos:", e)

    connection.commit()
    connection.close()

# Crear la tabla
crear_tabla(query1)

# Generar datos para la tabla de contabilidad
generar_datos_contabilidad(30)

# obtener_datos('contabilidad')