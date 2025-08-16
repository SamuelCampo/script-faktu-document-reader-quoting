import os
import json
import boto3
import google.generativeai as genai
# Importamos una librería para parsear URLs, es más seguro que cortar texto.
from urllib.parse import urlparse
import time
from datetime import datetime, timedelta

# --- CONFIGURACIÓN INICIAL (sin cambios) ---
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
genai.configure(api_key=GEMINI_API_KEY)

# Configurar cliente S3 con timeouts para evitar loops infinitos
s3_client = boto3.client('s3', 
    config=boto3.session.Config(
        connect_timeout=30,  # 30 segundos para conectar
        read_timeout=300,    # 5 minutos para leer
        retries={'max_attempts': 3}  # Máximo 3 reintentos
    )
)

def procesar_fechas_factura(invoice_date, due_date):
    """
    Procesa y valida las fechas de la factura.
    - Si no hay fecha de vencimiento, agrega 30 días a la fecha de emisión
    - Calcula los días de mora
    """
    try:
        fecha_actual = datetime.now()
        
        # Procesar fecha de emisión
        fecha_emision = None
        if invoice_date:
            try:
                fecha_emision = datetime.strptime(invoice_date, '%Y-%m-%d')
            except ValueError:
                print(f"ADVERTENCIA: Formato de fecha de emisión inválido: {invoice_date}")
                fecha_emision = None
        
        # Procesar fecha de vencimiento
        fecha_vencimiento = None
        if due_date:
            try:
                fecha_vencimiento = datetime.strptime(due_date, '%Y-%m-%d')
            except ValueError:
                print(f"ADVERTENCIA: Formato de fecha de vencimiento inválido: {due_date}")
                fecha_vencimiento = None
        
        # Si no hay fecha de vencimiento, calcular automáticamente (30 días después de emisión)
        if not fecha_vencimiento and fecha_emision:
            fecha_vencimiento = fecha_emision + timedelta(days=30)
            print(f"Fecha de vencimiento calculada automáticamente: {fecha_vencimiento.strftime('%Y-%m-%d')}")
        
        # Calcular días de mora
        dias_mora = 0
        if fecha_vencimiento:
            if fecha_actual > fecha_vencimiento:
                dias_mora = (fecha_actual - fecha_vencimiento).days
                print(f"Factura vencida. Días de mora: {dias_mora}")
            else:
                print("Factura no vencida. Días de mora: 0")
        
        return {
            'invoice_date': fecha_emision.strftime('%Y-%m-%d') if fecha_emision else None,
            'due_date': fecha_vencimiento.strftime('%Y-%m-%d') if fecha_vencimiento else None,
            'dias_mora': dias_mora,
            'fecha_actual': fecha_actual.strftime('%Y-%m-%d')
        }
        
    except Exception as e:
        print(f"Error procesando fechas: {str(e)}")
        return {
            'invoice_date': None,
            'due_date': None,
            'dias_mora': 0,
            'fecha_actual': datetime.now().strftime('%Y-%m-%d')
        }

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
        print(f"Bucket: {bucket_name}")
        print(f"File key: {file_key}")
        
        # Verificar si el archivo existe antes de intentar leerlo
        try:
            print("Verificando existencia del archivo...")
            s3_client.head_object(Bucket=bucket_name, Key=file_key)
            print("Archivo encontrado en S3")
        except Exception as head_error:
            print(f"Error al verificar archivo: {str(head_error)}")
            raise ValueError(f"El archivo {file_key} no existe en el bucket {bucket_name} o no tienes permisos para accederlo")
        
        # Leer el archivo con timeout
        try:
            print("Iniciando descarga del archivo...")
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            print("Archivo descargado exitosamente desde S3")
            
            # Leer el contenido con timeout
            file_content = response['Body'].read()
            file_size = len(file_content)
            print(f"Archivo leído exitosamente. Tamaño: {file_size} bytes")
            
        except Exception as s3_error:
            print(f"Error al leer archivo desde S3: {str(s3_error)}")
            raise ValueError(f"Error al descargar archivo desde S3: {str(s3_error)}")
        
        # Verificar si el archivo es muy grande
        if file_size > 10 * 1024 * 1024:  # 10MB
            print(f"ADVERTENCIA: Archivo muy grande ({file_size} bytes). Esto puede causar timeouts.")
        elif file_size == 0:
            raise ValueError("El archivo descargado está vacío")
        
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
        2.  Fecha de Vencimiento (due_date) - si no está especificada, déjala como null
        3.  Monto Total (total_amount)
        4.  Número de Factura (invoice_number) - si está disponible
        5.  Proveedor/Cliente (supplier_name) - si está disponible
        6.  Moneda (currency) - si está disponible

        Quiero que me devuelvas la información únicamente en formato JSON.
        El JSON debe tener la siguiente estructura: 
        {
            "invoice_date": "YYYY-MM-DD", 
            "due_date": "YYYY-MM-DD", 
            "total_amount": 0.00,
            "invoice_number": "string o null",
            "supplier_name": "string o null",
            "currency": "string o null"
        }
        
        - El monto total debe ser un número, sin símbolos de moneda.
        - Las fechas deben estar en formato AAAA-MM-DD.
        - Si no encuentras alguna de las fechas, usa null como valor.
        - Si no encuentras el monto, usa 0.0 como valor.
        - Si no encuentras algún campo opcional, usa null como valor.
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
            fecha_actual = datetime.now()
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Error al procesar con Gemini, devolviendo datos por defecto',
                    'extracted_data': {
                        'invoice_date': None,
                        'due_date': None,
                        'total_amount': 0.0,
                        'invoice_number': None,
                        'supplier_name': None,
                        'currency': None,
                        'dias_mora': 0,
                        'fecha_actual': fecha_actual.strftime('%Y-%m-%d')
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

        # 5. PROCESAR FECHAS Y CALCULAR DÍAS DE MORA
        print("Procesando fechas y calculando días de mora...")
        fechas_procesadas = procesar_fechas_factura(
            extracted_data.get('invoice_date'),
            extracted_data.get('due_date')
        )
        
        # Combinar datos extraídos con fechas procesadas
        resultado_final = {
            **extracted_data,
            'invoice_date': fechas_procesadas['invoice_date'],
            'due_date': fechas_procesadas['due_date'],
            'dias_mora': fechas_procesadas['dias_mora'],
            'fecha_actual': fechas_procesadas['fecha_actual']
        }
        
        print(f"Resultado final procesado: {json.dumps(resultado_final, indent=2)}")

        # 6. DEVOLVER UNA RESPUESTA EXITOSA (sin cambios)
        total_time = time.time() - start_time
        print(f"Procesamiento completado en {total_time:.2f} segundos")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Factura procesada exitosamente!',
                'extracted_data': resultado_final,
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