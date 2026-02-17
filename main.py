import os
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# --- Configuración (Basada en tu patrón) ---
INSTANCE_DB = os.getenv("INSTANCE_DB") # p.ej: project:region:instance
USER_DB = os.getenv("USER_DB")
PASSWORD_DB = os.getenv("PASSWORD_DB")
NAME_DB = os.getenv("NAME_DB") # Base de datos por defecto para la conexión inicial

# Esquemas y Flags
S_BIT = os.getenv("SCHEMA_BITCRAM")
S_ML = os.getenv("SCHEMA_ML")
S_APP = os.getenv("SCHEMA_APP")

def get_bool_env(key):
    return os.getenv(key, 'False').lower() == 'true'

# --- Lógica del Conector de Google ---
def getconn():
    connector = Connector()
    conn = connector.connect(
        INSTANCE_DB,
        "pymysql",
        user=USER_DB,
        password=PASSWORD_DB,
        db=NAME_DB,
    )
    return conn

# --- Inicialización del Engine ---
engine = create_engine(
    "mysql+pymysql://",
    creator=getconn,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=2,
)


def run_db_migration():
    with engine.connect() as conn:
        print("--- Iniciando migración dinámica de esquemas y tablas ---")
        
        # --- 0. Creación Condicional de ESQUEMAS ---
        
        if get_bool_env('CREATE_SCHEMA_BITCRAM'):
            print(f"Asegurando esquema: {S_BIT}...")
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{S_BIT}`;"))

        if get_bool_env('CREATE_SCHEMA_MERCADOLIBRE'):
            print(f"Asegurando esquema: {S_ML}...")
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{S_ML}`;"))

        if get_bool_env('CREATE_SCHEMA_APP'):
            print(f"Asegurando esquema: {S_APP}...")
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{S_APP}`;"))

        # Importante: Algunos conectores requieren un commit aquí si vas a usar 
        # las tablas en los mismos esquemas inmediatamente después.
        conn.commit() 

        # --- 1. RAW ITEM DATA ---
        if get_bool_env('CREATE_RAW_ITEM_DATA'):
            print(f"Creando {S_BIT}.raw_item_data...")
            conn.execute(text(f"DROP TABLE IF EXISTS `{S_BIT}`.`raw_item_data`;"))
            conn.execute(text(f"""
                CREATE TABLE `{S_BIT}`.`raw_item_data` (
                    `id` INT PRIMARY KEY,
                    `data` JSON,
                    `stock` INT,
                    `cost` INT,
                    `updated_at` DATETIME
                );
            """))

        # --- 2. PRODUCT STATUS ---
        if get_bool_env('CREATE_PRODUCT_STATUS'):
            print(f"Creando {S_ML}.product_status...")
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{S_ML}`.`product_status` (
                    `meli_id` VARCHAR(50) NOT NULL PRIMARY KEY,
                    `product_name` VARCHAR(255),
                    `stock` INT,
                    `status` VARCHAR(50),
                    `reason` VARCHAR(255),
                    `remedy` VARCHAR(255),
                    `updated_at` DATETIME
                );
            """))

        # --- 3. PRODUCT CATALOG SYNC ---
        if get_bool_env('CREATE_PRODUCT_CATALOG_SYNC'):
            print(f"Creando {S_APP}.product_catalog_sync...")
            conn.execute(text(f"DROP TABLE IF EXISTS `{S_APP}`.`product_catalog_sync`;"))
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{S_APP}`.`product_catalog_sync` (
                    `id` INT NOT NULL PRIMARY KEY,
                    `price` DECIMAL(10,2),
                    `product_code` VARCHAR(255),
                    `product_name` VARCHAR(255),
                    `product_image_b_format_url` TEXT,
                    `product_type_id` VARCHAR(255),
                    `product_type_path` VARCHAR(255),
                    `product_use_stock` VARCHAR(50),
                    `product_sale_type_id` VARCHAR(50),
                    `product_search_codes` TEXT,
                    `product_type_node_left` VARCHAR(50),
                    `product_change_cost_on_sales` VARCHAR(50),
                    `stock` INTEGER,
                    `cost` DECIMAL(10,2),
                    `title_meli` VARCHAR(255),
                    `description` TEXT,
                    `brand` VARCHAR(255),
                    `meli_id` VARCHAR(50),
                    `drive_url` VARCHAR(255),
                    `status` VARCHAR(50),
                    `reason` VARCHAR(255),
                    `remedy` VARCHAR(255)
                );
            """))

        # --- 4. PROMPTS ---
        if get_bool_env('CREATE_PROMPTS'):
            print(f"Creando {S_ML}.prompts...")
            conn.execute(text(f"DROP TABLE IF EXISTS `{S_ML}`.`prompts`;"))
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{S_ML}`.`prompts` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `ai_auditor` TEXT,
                    `ai_category` TEXT,
                    `ai_general` TEXT,
                    `ai_inventory_search` TEXT,
                    `ai_recommendation` TEXT,
                    `ai_improving_human_reply` TEXT
                );
            """))
            conn.execute(text(f"INSERT INTO `{S_ML}`.`prompts` (id) VALUES (0);"))

        # --- 5. ORDERS ---
        if get_bool_env('CREATE_ORDERS'):
            print(f"Creando {S_ML}.orders...")
            conn.execute(text(f"DROP TABLE IF EXISTS `{S_ML}`.`orders`;"))
            conn.execute(text(f"""
                CREATE TABLE `{S_ML}`.`orders` (
                  `id` VARCHAR(255) NOT NULL PRIMARY KEY,
                  `data` JSON,
                  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))

        # --- 6. SCRAPPED COMPETENCE ---
        if get_bool_env('CREATE_SCRAPPED_COMPETENCE'):
            print(f"Creando {S_ML}.scrapped_competence...")
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{S_ML}`.`scrapped_competence` (
                   `meli_id` VARCHAR (50) PRIMARY KEY,
                   `url` TEXT NOT NULL,
                   `title` VARCHAR(255),
                   `price` DECIMAL(10, 2),
                   `competitor` VARCHAR(100),
                   `price_in_installments` VARCHAR(100),
                   `image` TEXT,
                   `timestamp` DATETIME,
                   `status` VARCHAR(50),
                   `api_cost_total` DECIMAL(10, 4),
                   `remaining_credits` INT,
                   `product_code` VARCHAR(100),
                   `product_name` VARCHAR(255)
                );
            """))
        conn.commit()
        print("--- Migración completada exitosamente ---")
        

        
if __name__ == "__main__":
    run_db_migration()