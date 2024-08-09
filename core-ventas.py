import datetime
import psycopg2
import random
# import datetime
from faker import Faker
from faker.providers import address, internet

# Crear instancia de Faker con configuración en español
fake = Faker('es_CL')

# Se crea una segunda instancia de Faker, para poder generar los mismos datos en las categorías que lo necesiten
fake2 = Faker('es_CL')
fake2.seed_instance(12345)

# Conexión a la base de datos en postgres
connection = psycopg2.connect(
    user="taller2",
    password="joaquintorres1@",
    host="taller2clientes.postgres.database.azure.com",
    port="5432",
    database="core-clientes"
)

connection.autocommit = True

# Queries para crear tablas
query1 = """CREATE TABLE IF NOT EXISTS ventas (
    id_venta SERIAL PRIMARY KEY,
    rut VARCHAR(12) NOT NULL,
    codigo_barra VARCHAR(13) NOT NULL,
    fecha_venta DATE NOT NULL,
    hora_venta TIME NOT NULL,
    cantidad INT NOT NULL,
    precio_unitario INT NOT NULL,
    total_venta INT NOT NULL,
    descuento INT,
    forma_pago VARCHAR(255),
    notas VARCHAR(500)
    )"""

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
    rut = fake2.random_int(1000000, 39999999)
    # Calcular el dígito verificador
    suma = 0
    multiplicador = 2
    for digito in str(rut)[::-1]:
        suma += int(digito) * multiplicador
        multiplicador = multiplicador + 1 if multiplicador < 7 else 2
    dv = 11 - (suma % 11)
    dv = 'k' if dv == 10 else '0' if dv == 11 else str(dv)
    
    return f"{rut}-{dv}"

# Función que genera datos para la tabla de ventas
def generar_datos_ventas(cantidad):
    cursor = connection.cursor()
    for _ in range(cantidad):
        rut = generar_rut()
        codigo_barra = fake.ean13()
        fecha_venta = fake.date()
        hora_venta = fake.time()
        cantidad = random.randint(1, 10)
        precio_unitario = random.randint(1000, 150000)
        total_venta = cantidad * precio_unitario
        descuento = random.randint(0, 100)
        forma_pago = random.choice(["Efectivo", "Transferencia", "Tarjeta de crédito"])
        notas = fake.text()

        # Crear diccionario de datos
        datos_ventas = {
            'rut': rut,
            'codigo_barra': codigo_barra,
            'fecha_venta': fecha_venta,
            'hora_venta': hora_venta,
            'cantidad': cantidad,
            'precio_unitario': precio_unitario,
            'total_venta': total_venta,
            'descuento': descuento,
            'forma_pago': forma_pago,
            'notas': notas
        }

        # Insertar datos en la tabla
        query = """INSERT INTO ventas (rut, codigo_barra, fecha_venta, hora_venta, cantidad, precio_unitario, total_venta, descuento, forma_pago, notas)
        VALUES (%(rut)s, %(codigo_barra)s, %(fecha_venta)s, %(hora_venta)s, %(cantidad)s, %(precio_unitario)s, %(total_venta)s, %(descuento)s, %(forma_pago)s, %(notas)s)"""

        try:
            cursor.execute(query, datos_ventas)
        except Exception as e:
            print("Error al insertar los datos:", e)
        
    connection.commit()
    cursor.close()

# Crear la tabla
crear_tabla(query1)

# Generar datos para la tabla de ventas
generar_datos_ventas(30)

#obtener_datos('ventas')

#eliminar_tabla('ventas')