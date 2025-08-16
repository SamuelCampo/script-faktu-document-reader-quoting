import os
import json
import boto3
import google.generativeai as genai
# Importamos una librería para parsear URLs, es más seguro que cortar texto.
from urllib.parse import urlparse
import time

# --- CONFIGURACIÓN INICIAL (sin cambios) ---
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
genai.configure(api_key=GEMINI_API_KEY)
s3_client = boto3.client('s3')

def handler(event, context):
    """
    Función principal que se ejecuta en Lambda.
    Ahora espera un parámetro 'path' en el 'event' con la ruta S3 del archivo.
    Ejemplo de 'event': {"path": "s3://mi-bucket/facturas/factura-123.pdf"}
    """
    start_time = time.time()
    print(f"Iniciando procesamiento de factura desde un path específico... Tiempo límite: {context.get_remaining_time_in_millis()}ms")

    try:
        # <<< CAMBIO: Inicio de la nueva lógica para leer el path >>>

        # 1. OBTENER LA RUTA DEL ARCHIVO DESDE EL PARÁMETRO 'path'
        s3_path = event.get('path')
        if not s3_path:
            raise ValueError("Error: El parámetro 'path' no se encontró en el evento.")
        
        print(f"Path S3 recibido: {s3_path}")

        # Usamos urlparse para dividir la ruta S3 en sus componentes de forma segura.
        parsed_url = urlparse(s3_path)
        if parsed_url.scheme != 's3':
            raise ValueError("Error: El path debe ser una URI de S3 válida (ej: s3://bucket/archivo.pdf)")
            
        bucket_name = parsed_url.netloc  # El 'netloc' es el nombre del bucket
        file_key = parsed_url.path.lstrip('/') # El 'path' es la ruta del archivo, quitamos el '/' inicial

        if not bucket_name or not file_key:
            raise ValueError("Error: La URI de S3 no es válida. No se pudo extraer el bucket o el archivo.")

        # <<< CAMBIO: Fin de la nueva lógica >>>
        
        print(f"Archivo a procesar: {file_key} en el bucket: {bucket_name}")

        # 2. LEER EL ARCHIVO DESDE S3 (sin cambios)
        print("Leyendo archivo desde S3...")
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read()
        file_size = len(file_content)
        print(f"Archivo leído exitosamente. Tamaño: {file_size} bytes")
        
        # Verificar si el archivo es muy grande
        if file_size > 10 * 1024 * 1024:  # 10MB
            print(f"ADVERTENCIA: Archivo muy grande ({file_size} bytes). Esto puede causar timeouts.")
        
        file_type = file_key.split('.')[-1].lower()
        if file_type == 'jpg':
            file_type = 'jpeg'
        
        mime_type = f"image/{file_type}"
        if file_type == 'pdf':
            mime_type = "application/pdf"

        # 3. PREPARAR Y ENVIAR LA SOLICITUD A GEMINI (sin cambios)
        print("Configurando modelo Gemini...")
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = """
        Eres un asistente experto en analizar facturas.
        Por favor, analiza el siguiente documento y extrae la siguiente información:
        1.  Fecha de Emisión (invoice_date)
        2.  Fecha de Vencimiento (due_date)
        3.  Monto Total (total_amount)

        Quiero que me devuelvas la información únicamente en formato JSON.
        El JSON debe tener la siguiente estructura: {"invoice_date": "YYYY-MM-DD", "due_date": "YYYY-MM-DD", "total_amount": 0.00}
        
        - El monto total debe ser un número, sin símbolos de moneda.
        - Las fechas deben estar en formato AAAA-MM-DD.
        - Si no encuentras alguna de las fechas, usa null como valor.
        - Si no encuentras el monto, usa 0.0 como valor.
        """
        invoice_file = {'mime_type': mime_type, 'data': file_content}
        
        print("Enviando el documento a Gemini para su análisis...")
        print(f"Tiempo restante antes de la llamada: {context.get_remaining_time_in_millis()}ms")
        
        # Agregar timeout y mejor manejo de errores
        try:
            response_gemini = model.generate_content([prompt, invoice_file])
            print("Respuesta recibida de Gemini exitosamente")
        except Exception as gemini_error:
            print(f"Error en la llamada a Gemini: {str(gemini_error)}")
            # Si falla Gemini, devolver datos por defecto
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Error al procesar con Gemini, devolviendo datos por defecto',
                    'extracted_data': {
                        'invoice_date': None,
                        'due_date': None,
                        'total_amount': 0.0
                    },
                    'gemini_error': str(gemini_error)
                })
            }

        # 4. PROCESAR LA RESPUESTA DE GEMINI (sin cambios)
        raw_json_text = response_gemini.text
        print(f"Respuesta recibida de Gemini: {raw_json_text}")
        cleaned_json_text = raw_json_text.strip().replace('```json', '').replace('```', '').strip()
        extracted_data = json.loads(cleaned_json_text)
        print(f"Datos extraídos y formateados: {json.dumps(extracted_data, indent=2)}")

        # 5. DEVOLVER UNA RESPUESTA EXITOSA (sin cambios)
        total_time = time.time() - start_time
        print(f"Procesamiento completado en {total_time:.2f} segundos")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Factura procesada exitosamente!',
                'extracted_data': extracted_data,
                'processing_time': total_time
            })
        }

    except Exception as e:
        # GESTIÓN DE ERRORES (sin cambios)
        total_time = time.time() - start_time
        print(f"Ha ocurrido un error después de {total_time:.2f} segundos: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'processing_time': total_time
            })
        }