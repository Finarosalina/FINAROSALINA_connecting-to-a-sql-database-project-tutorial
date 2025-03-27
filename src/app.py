import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar las variables de entorno
load_dotenv()

# 1) Conectar a la base de datos usando SQLAlchemy
def connect():
    global engine
    try:
        # Crea el string de conexión
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Iniciando la conexión...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("¡Conexión exitosa!")
        return engine
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# 2) Crear las tablas (sin cambios)
def create_tables():
    try:
        with engine.connect() as connection:
            create_table_queries = [
                """
                CREATE TABLE IF NOT EXISTS publishers (
                    publisher_id INT NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    PRIMARY KEY(publisher_id)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS authors (
                    author_id INT NOT NULL,
                    first_name VARCHAR(100) NOT NULL,
                    middle_name VARCHAR(50),
                    last_name VARCHAR(100),
                    PRIMARY KEY(author_id)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS books (
                    book_id INT NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    total_pages INT,
                    rating DECIMAL(4, 2),
                    isbn VARCHAR(13),
                    published_date DATE,
                    publisher_id INT,
                    PRIMARY KEY(book_id),
                    CONSTRAINT fk_publisher FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS book_authors (
                    book_id INT NOT NULL,
                    author_id INT NOT NULL,
                    PRIMARY KEY(book_id, author_id),
                    CONSTRAINT fk_book FOREIGN KEY(book_id) REFERENCES books(book_id) ON DELETE CASCADE,
                    CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(author_id) ON DELETE CASCADE
                );
                """
            ]
            
            # Ejecutar todas las consultas para crear las tablas
            for query in create_table_queries:
                connection.execute(text(query))  # Ejecutar la consulta SQL
            print("¡Tablas creadas exitosamente!")
    
    except Exception as e:
        print(f"Error al crear las tablas: {e}")

# 3) Insertar datos (sin cambios)
def insert_data():
    try:
        with engine.connect() as connection:
            insert_query = """
            -- publishers
            INSERT INTO publishers(publisher_id, name) 
            VALUES (1, 'O Reilly Media') 
            ON CONFLICT (publisher_id) DO NOTHING;

            INSERT INTO publishers(publisher_id, name) 
            VALUES (2, 'A Book Apart') 
            ON CONFLICT (publisher_id) DO NOTHING;

            --... más inserciones
            """
            connection.execute(text(insert_query))
            print("¡Datos insertados exitosamente!")
    except Exception as e:
        print(f"Error al insertar los datos: {e}")

# 4) Leer y mostrar los datos de cada tabla usando pandas

import pandas as pd

def get_tables_and_data():
    try:
        with engine.connect() as connection:
            # Obtener todas las tablas
            tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
            tables = pd.read_sql(tables_query, connection)
            print("Tablas disponibles:")
            print(tables)

            # Para cada tabla, obtener sus columnas y los datos
            for table in tables['table_name']:
                # Mostrar columnas
                columns_query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{table}';
                """
                columns = pd.read_sql(columns_query, connection)
                print(f"\nColumnas de la tabla {table}:")
                print(columns)

                # Mostrar los primeros 5 registros de la tabla (para no cargar todo si son muchos)
                data_query = f"SELECT * FROM {table} LIMIT 5;"  # Usamos LIMIT 5 para ver solo unos pocos registros
                data = pd.read_sql(data_query, connection)
                print(f"Primeros 5 registros de la tabla {table}:")
                print(data)
                print("\n" + "-"*40)
    except Exception as e:
        print(f"Error al obtener las tablas y datos: {e}")
