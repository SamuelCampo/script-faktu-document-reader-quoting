import os
import json
import boto3
import google.generativeai as genai
# Importamos una librería para parsear URLs, es más seguro que cortar texto.
from urllib.parse import urlparse
import time
from datetime import datetime, timedelta
import requests

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

def validar_formato_rut(rut_string):
    """
    Valida y formatea RUTs chilenos.
    Acepta formatos: 12.345.678-9, 12345678-9, 123456789
    Retorna el RUT formateado o None si es inválido.
    """
    if not rut_string:
        return None
    
    try:
        # Limpiar el RUT de espacios y puntos
        rut_limpio = str(rut_string).replace('.', '').replace(' ', '').strip()
        
        # Verificar que tenga el formato básico
        if '-' in rut_limpio:
            # Formato con guión: 12345678-9
            numero, dv = rut_limpio.split('-')
        else:
            # Formato sin guión: 123456789
            if len(rut_limpio) < 2:
                return None
            numero = rut_limpio[:-1]
            dv = rut_limpio[-1]
        
        # Validar que el número sea numérico
        if not numero.isdigit():
            return None
        
        # Validar longitud del número (7-8 dígitos)
        if len(numero) < 7 or len(numero) > 8:
            return None
        
        # Validar que el dígito verificador sea alfanumérico
        if not (dv.isdigit() or dv.upper() == 'K'):
            return None
        
        # Formatear como 12.345.678-9
        numero_int = int(numero)
        if numero_int < 1000000:  # Menos de 7 dígitos
            return None
            
        # Agregar puntos cada 3 dígitos desde la derecha
        numero_str = str(numero_int)
        numero_formateado = ''
        for i, digito in enumerate(reversed(numero_str)):
            if i > 0 and i % 3 == 0:
                numero_formateado = '.' + numero_formateado
            numero_formateado = digito + numero_formateado
        
        return f"{numero_formateado}-{dv.upper()}"
        
    except Exception as e:
        print(f"Error validando RUT '{rut_string}': {str(e)}")
        return None

def procesar_datos_factura(datos_extraidos):
    """
    Procesa y valida los datos extraídos de la factura.
    - Valida formato de RUTs
    - Asegura que los campos estén presentes
    """
    try:
        # Validar RUTs
        supplier_rut = validar_formato_rut(datos_extraidos.get('supplier_rut'))
        customer_rut = validar_formato_rut(datos_extraidos.get('customer_rut'))
        
        # Log de validación de RUTs
        if supplier_rut:
            print(f"RUT del proveedor validado: {supplier_rut}")
        else:
            print("ADVERTENCIA: RUT del proveedor inválido o no encontrado")
            
        if customer_rut:
            print(f"RUT del cliente validado: {customer_rut}")
        else:
            print("ADVERTENCIA: RUT del cliente inválido o no encontrado")
        
        # Asegurar que todos los campos estén presentes
        datos_procesados = {
            'invoice_date': datos_extraidos.get('invoice_date'),
            'due_date': datos_extraidos.get('due_date'),
            'total_amount': datos_extraidos.get('total_amount', 0.0),
            'invoice_number': datos_extraidos.get('invoice_number'),
            'currency': datos_extraidos.get('currency'),
            'supplier_name': datos_extraidos.get('supplier_name'),
            'supplier_rut': supplier_rut,
            'supplier_address': datos_extraidos.get('supplier_address'),
            'supplier_giro': datos_extraidos.get('supplier_giro'),
            'supplier_ciudad': datos_extraidos.get('supplier_ciudad'),
            'supplier_comuna': datos_extraidos.get('supplier_comuna'),
            'customer_name': datos_extraidos.get('customer_name'),
            'customer_rut': customer_rut,
            'customer_address': datos_extraidos.get('customer_address'),
            'customer_giro': datos_extraidos.get('customer_giro'),
            'customer_comuna': datos_extraidos.get('customer_comuna'),
            'customer_ciudad': datos_extraidos.get('customer_ciudad'),
            'purchase_order': datos_extraidos.get('purchase_order'),
            'reference_folio': datos_extraidos.get('reference_folio')
        }
        
        return datos_procesados
        
    except Exception as e:
        print(f"Error procesando datos de la factura: {str(e)}")
        return datos_extraidos

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
        quoting_batch_id = event.get('quoting_batch_id')
        environment = event.get('environment', 'production')
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
        model = genai.GenerativeModel('gemini-2.5-flash')
        prompt = """
        Eres un asistente experto en analizar facturas chilenas.
        Por favor, analiza el siguiente documento y extrae la siguiente información:
        
        INFORMACIÓN BÁSICA:
        1.  Fecha de Emisión (invoice_date)
        2.  Fecha de Vencimiento (due_date) - si no está especificada, déjala como null
        3.  Monto Total (total_amount)
        4.  Número de Factura (invoice_number) - si está disponible
        5.  Moneda (currency) - si está disponible
        
        INFORMACIÓN DEL PROVEEDOR:
        6.  Nombre del Proveedor (supplier_name) - empresa que emite la factura
        7.  RUT del Proveedor (supplier_rut) - número de identificación fiscal del proveedor
        8.  Dirección del Proveedor (supplier_address) - si está disponible
        9.  Giro del Proveedor (supplier_giro) - si está disponible
        10. Ciudad del Proveedor (supplier_ciudad) - si está disponible
        11. Comuna del Proveedor (supplier_comuna) - si está disponible
        
        INFORMACIÓN DEL CLIENTE:
        12. Nombre del Cliente (customer_name) - empresa/persona a quien se emite la factura
        12. RUT del Cliente (customer_rut) - número de identificación fiscal del cliente
        13. Dirección del Cliente (customer_address) - si está disponible
        14. Giro del Cliente (customer_giro) - si está disponible
        15. Comuna del Cliente (customer_comuna) - si está disponible
        16. Ciudad del Cliente (customer_ciudad) - si está disponible

        OTROS DATOS:
        17. Orden de Compra (purchase_order) - si está disponible
        18. Folio de la referencia (reference_folio) - si está disponible

        Quiero que me devuelvas la información únicamente en formato JSON.
        El JSON debe tener la siguiente estructura: 
        {
            "invoice_date": "YYYY-MM-DD", 
            "due_date": "YYYY-MM-DD", 
            "total_amount": 0.00,
            "invoice_number": "string o null",
            "currency": "string o null",
            "supplier_name": "string o null",
            "supplier_rut": "string o null",
            "supplier_address": "string o null",
            "supplier_giro": "string o null",
            "supplier_ciudad": "string o null",
            "supplier_comuna": "string o null",
            "customer_name": "string o null",
            "customer_rut": "string o null",
            "customer_address": "string o null",
            "customer_giro": "string o null",
            "customer_comuna": "string o null",
            "customer_ciudad": "string o null",
            "purchase_order": "string o null",
            "reference_folio": "string o null"
        }
        
        - El monto total debe ser un número, sin símbolos de moneda.
        - Las fechas deben estar en formato AAAA-MM-DD.
        - Los RUTs deben estar en formato estándar chileno (ej: 12.345.678-9 o 12345678-9).
        - Si no encuentras alguna de las fechas, usa null como valor.
        - Si no encuentras el monto, usa 0.0 como valor.
        - Si no encuentras algún campo opcional, usa null como valor.
        - En Chile, el proveedor es quien EMITE la factura y el cliente es quien LA RECIBE.
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
                        'currency': None,
                        'supplier_name': None,
                        'supplier_rut': None,
                        'supplier_address': None,
                        'supplier_giro': None,
                        'supplier_ciudad': None,
                        'supplier_comuna': None,
                        'customer_name': None,
                        'customer_rut': None,
                        'customer_address': None,
                        'customer_giro': None,
                        'customer_comuna': None,
                        'customer_ciudad': None,
                        'purchase_order': None,
                        'reference_folio': None,
                        'dias_mora': 0,
                        'fecha_actual': fecha_actual.strftime('%Y-%m-%d'),
                        'quoting_batch_id': quoting_batch_id,
                        'path_document': file_key
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

        # 5. VALIDAR Y PROCESAR DATOS DE LA FACTURA
        print("Validando y procesando datos de la factura...")
        datos_validados = procesar_datos_factura(extracted_data)
        print(f"Datos validados: {json.dumps(datos_validados, indent=2)}")

        # 6. PROCESAR FECHAS Y CALCULAR DÍAS DE MORA
        print("Procesando fechas y calculando días de mora...")
        fechas_procesadas = procesar_fechas_factura(
            datos_validados.get('invoice_date'),
            datos_validados.get('due_date')
        )
        
        # Combinar datos validados con fechas procesadas
        resultado_final = {
            **datos_validados,
            'invoice_date': fechas_procesadas['invoice_date'],
            'due_date': fechas_procesadas['due_date'],
            'dias_mora': fechas_procesadas['dias_mora'],
            'fecha_actual': fechas_procesadas['fecha_actual'],
            'quoting_batch_id': quoting_batch_id,
            'path_document': file_key
        }
        
        print(f"Resultado final procesado: {json.dumps(resultado_final, indent=2)}")

        # vamos a enviar un esta data por post a una url que viene de enviroment que esta esperando un webhook

        if environment == 'development':
            webhook_url =  os.environ.get('WEBHOOK_URL_DEV')
        else:
            webhook_url =  os.environ.get('WEBHOOK_URL')
        print(f"Enviando datos al webhook: {webhook_url}")
        if webhook_url:
            try:
                headers = {'Content-Type': 'application/json'}
                # Agregar timeout para evitar que se quede colgado
                # timeout=(connect_timeout, read_timeout) en segundos
                timeout_config = (10, 30)  # 10 segundos para conectar, 30 segundos para recibir respuesta
                print(f"Enviando POST con timeout: {timeout_config[0]}s conexión, {timeout_config[1]}s lectura")
                
                response = requests.post(
                    webhook_url, 
                    json=resultado_final, 
                    headers=headers,
                    timeout=timeout_config
                )
                print(f"Webhook response status: {response.status_code}, body: {response.text}")
                response.raise_for_status()
                print(f"Datos enviados al webhook exitosamente: {response.text}")
            except requests.Timeout as e:
                print(f"TIMEOUT al enviar datos al webhook (se agotó el tiempo de espera): {str(e)}")
                print(f"El servidor en {webhook_url} no respondió en el tiempo esperado. Continuando con el procesamiento...")
            except requests.ConnectionError as e:
                print(f"ERROR DE CONEXIÓN al enviar datos al webhook (no se pudo conectar al servidor): {str(e)}")
                print(f"Verifica que el servidor en {webhook_url} esté corriendo y accesible desde el contenedor Docker.")
                print(f"Si estás usando host.docker.internal, asegúrate de que Docker tenga permisos de red.")
            except requests.RequestException as e:
                print(f"Error al enviar datos al webhook: {str(e)}")
                print(f"Tipo de error: {type(e).__name__}")

        # 7. DEVOLVER UNA RESPUESTA EXITOSA (sin cambios)
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