import os
import logging
import sqlite3
import hashlib
import asyncio
import numpy as np
from io import BytesIO
from PIL import Image, ImageFilter, UnidentifiedImageError
from telegram import Update
from telegram.ext import Application, MessageHandler, filters
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

# 🔧 Configuración avanzada
TOKEN = "7910942735:AAHVLnkTrljFdxIktYCBzD0rJPlby4RGSiE"
FOLDER_ID = "1dtP5Ls8JUmmqd2bg7GvcZ8yrj3FnihmH"
SERVICE_ACCOUNT_FILE = "comprobantes-454108-a4e69bc8d002.json"
DB_NAME = "comprobantes.db"
SCOPES = ["https://www.googleapis.com/auth/drive"]

# 🖼️ Parámetros de normalización de imágenes
IMG_STANDARD_SIZE = (800, 800)  # Tamaño estándar para redimensionar
IMG_QUALITY = 75  # Calidad JPEG de normalización

# 🚀 Inicialización
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO)
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build("drive", "v3", credentials=creds)


# 🗄️ Base de Datos mejorada
def init_db():
    """Inicializa la base de datos con la estructura correcta."""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()

        # Verificar si la tabla ya existe
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='comprobantes'"
        )
        table_exists = cursor.fetchone()

        if table_exists:
            # Verificar si la columna 'phash' existe
            cursor.execute("PRAGMA table_info(comprobantes)")
            columns = cursor.fetchall()
            column_names = [column[1] for column in columns]

            if 'phash' not in column_names:
                # Si la tabla existe pero no tiene la columna 'phash', la recreamos
                cursor.execute("DROP TABLE comprobantes")
                table_exists = False

        if not table_exists:
            # Crear la tabla con la estructura correcta
            cursor.execute('''CREATE TABLE comprobantes
                           (phash TEXT PRIMARY KEY, drive_id TEXT, fecha TIMESTAMP)'''
                           )
            conn.commit()
            logging.info("Tabla 'comprobantes' creada correctamente.")
        else:
            logging.info("Tabla 'comprobantes' ya existe.")


init_db()


async def procesar_imagen(file_data: bytes) -> bytes:
    """Normaliza imágenes para comparación consistente"""
    try:
        img = Image.open(BytesIO(file_data))

        # 1. Conversión a escala de grises
        img = img.convert("L")

        # 2. Reducción de ruido de compresión
        img = img.filter(ImageFilter.SMOOTH_MORE)

        # 3. Redimensionado inteligente
        img.thumbnail(IMG_STANDARD_SIZE, Image.Resampling.LANCZOS)

        # 4. Normalización de histograma
        img = img.point(lambda x: 0 if x < 30 else 255 if x > 225 else x)

        # Guardar como JPEG estandarizado
        buffer = BytesIO()
        img.save(buffer, "JPEG", quality=IMG_QUALITY, optimize=True)
        return buffer.getvalue()

    except UnidentifiedImageError:
        raise ValueError("El archivo no es una imagen válida.")
    except Exception as e:
        raise ValueError(f"Error procesando la imagen: {str(e)}")


async def generar_phash(file_data: bytes) -> str:
    """Genera hash perceptual de la imagen normalizada"""
    try:
        processed_data = await procesar_imagen(file_data)

        # Calcular histograma como vector de características
        img = Image.open(BytesIO(processed_data))
        histogram = img.histogram()

        # Reducir dimensionalidad
        simplified = [int(sum(histogram[i:i + 16])) for i in range(0, 256, 16)]

        # Generar hash único
        avg = np.mean(simplified)
        phash = ''.join(['1' if i > avg else '0' for i in simplified])
        return phash

    except Exception as e:
        raise ValueError(f"Error generando hash perceptual: {str(e)}")


async def verificar_duplicado(phash: str) -> bool:
    """Verifica en la base de datos"""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            return conn.execute("SELECT 1 FROM comprobantes WHERE phash=?",
                                (phash, )).fetchone() is not None
    except Exception as e:
        raise ValueError(f"Error verificando duplicados: {str(e)}")


async def subir_y_registrar(file_data: bytes) -> str:
    """Sube a Drive y registra en DB"""
    try:
        phash = await generar_phash(file_data)

        if await verificar_duplicado(phash):
            raise ValueError(
                "🔄 Comprobante duplicado (mismo contenido visual)")

        # Subir a Drive
        file_metadata = {
            "name": f"comprobante_{phash[:12]}.jpg",
            "parents": [FOLDER_ID]
        }
        media = MediaIoBaseUpload(BytesIO(file_data), mimetype="image/jpeg")
        drive_id = drive_service.files().create(body=file_metadata,
                                                media_body=media,
                                                fields="id").execute()["id"]

        # Registrar en DB
        with sqlite3.connect(DB_NAME) as conn:
            conn.execute(
                "INSERT INTO comprobantes VALUES (?, ?, datetime('now'))",
                (phash, drive_id))

        return drive_id

    except HttpError as e:
        raise ValueError(f"Error subiendo a Google Drive: {e._get_reason()}")
    except Exception as e:
        raise ValueError(f"Error registrando el comprobante: {str(e)}")


async def manejar_comprobante(update: Update, context):
    try:
        # Detectar archivo o imagen
        file = None
        if update.message.document:
            file = update.message.document
        elif update.message.photo:
            file = update.message.photo[-1]

        if not file:
            await update.message.reply_text(
                "❌ No se detectó ningún archivo válido.")
            return

        # Descargar y procesar
        file_data = await (await
                           context.bot.get_file(file.file_id
                                                )).download_as_bytearray()
        await subir_y_registrar(file_data)
        

    except ValueError as e:
        await update.message.reply_text(f"⚠️ {str(e)}")
    except Exception as e:
        logging.error(f"Error crítico: {str(e)}", exc_info=True)
        await update.message.reply_text(
            "🔥 Error interno del sistema. Inténtalo de nuevo.")


def main():
    application = Application.builder().token(TOKEN).build()
    application.add_handler(
        MessageHandler(filters.Document.IMAGE | filters.PHOTO,
                       manejar_comprobante))
    application.run_polling()


if __name__ == "__main__":
    main()
